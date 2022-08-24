-module(ext).
-include("ext.hrl").

%% Library lifetime management
-export([start/0,
         stop/0]).

%% Creation API
-export([default/4,
         new/5]).

-export([ping/3]).

%% Start API
%% TODO(borja): Add more for read-only, snapshot transactions
-export([start_transaction/2]).

%% Sync read / update API
-export([sync_read/3,
         sync_update/4]).

%% Async read / update API
-export([async_read/3,
         await_read/3,
         async_update/4,
         await_update/3]).

%% Commit / Release API
-export([async_commit/3,
         await_commit/2,
         commit/2,
         commit/3,
         commit_at/3,
         release/2]).

-record(coordinator, {
    %% The IP we're using to talk to the server
    %% Used to create a transaction id
    self_ip :: binary(),

    %% Routing info
    ring :: ext_ring:ext_ring(),

    %% Opened pool of connections, one pool per node in the cluster
    conn_pool :: #{node_and_port() => shackle_pool()},

    tx_id_prefix :: binary()
}).

-record(transaction, {
    id :: binary(),
    timestamp :: timestamp(),
    init_index_node = undefined :: undefined | index_node(),

    ballots = #{} :: #{partition_id() => ballot()},
    leaders = #{} :: #{index_node() => replica_id()},
    partitions = #{} :: #{index_node() => []}
}).

-opaque t() :: #coordinator{}.
-opaque tx() :: #transaction{}.

-opaque read_req_id() :: {read, shackle:external_request_id(), index_node()}.
-opaque update_req_id() :: {update, shackle:external_request_id(), index_node()}.
-opaque commit_req_id() :: {commit, shackle:external_request_id()} | empty_commit.

-export_type([t/0, tx/0, read_req_id/0, update_req_id/0, commit_req_id/0]).

%%====================================================================
%% Library APP functions
%%====================================================================

-spec start() -> ok | {error, Reason :: term()}.
start() ->
    {ok, _} = application:ensure_all_started(shackle),
    ok.

-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    application:stop(shackle).

%%====================================================================
%% Client Creation
%%====================================================================

-spec default(CoordId :: non_neg_integer(),
              ReplicaId :: replica_id(),
              MasterIp :: node_ip(),
              MasterPort :: inet:port_number()) -> {ok, t()} | {error, term()}.

default(CoordId, ReplicaID, MasterIp, MasterPort) ->
    ext:start(),
    case ext_ring:replica_info(ReplicaID, MasterIp, MasterPort) of
        {error, Reason} ->
            {error, Reason};
        {ok, LocalIP, Ring, LocalNodes} ->
            Pools = lists:foldl(
                fun({_, IP, Port}, Acc) ->
                    Name = list_to_atom(IP ++ "_" ++ integer_to_list(Port) ++ "_shackle_pool"),
                    ok = shackle_pool:start(
                        Name,
                        ext_shackle_transport,
                        [
                            {address, IP}, {port, Port}, {reconnect, false},
                            {socket_options, [{packet, 4}, binary, {nodelay, true}]},
                            {init_options, #{}}
                        ],
                        [
                            {pool_size, erlang:system_info(schedulers_online)},
                            {backlog_size, infinity},
                            {max_retries, infinity}
                        ]
                    ),
                    Acc#{{IP, Port} => Name}
                end,
                #{},
                LocalNodes
            ),
            new(ReplicaID, LocalIP, CoordId, Ring, Pools)
    end.

-spec new(ReplicaId :: replica_id(),
          LocalIP :: node_ip(),
          WorkerId :: non_neg_integer(),
          RingInfo :: ext_ring:t(),
          NodePool :: #{node_and_port() => shackle_pool()}) -> {ok, t()}.

new(ReplicaId, LocalIP, WorkerId, RingInfo, NodePool) ->
    SelfIP = list_to_binary(inet:ntoa(LocalIP)),
    {ok, #coordinator{self_ip=SelfIP,
                      ring=RingInfo,
                      conn_pool=NodePool,
                      tx_id_prefix=make_id_prefix(ReplicaId, SelfIP, WorkerId)}}.

%%====================================================================
%% Client API functions
%%====================================================================

-spec start_transaction(t(), non_neg_integer()) -> {ok, tx()}.
start_transaction(#coordinator{tx_id_prefix=Prefix}, Id) ->
    {ok, #transaction{id=make_id_from_prefix(Prefix, Id),
                      timestamp=erlang:system_time(nanosecond)}}.

-spec async_commit(t(), tx(), timeout()) -> {ok, commit_req_id()}.
async_commit(_, #transaction{init_index_node=undefined}, _) ->
    %% If the transaction never read anything, we bypass 2PC
    {ok, empty_commit};

async_commit(#coordinator{conn_pool=Pools},
             #transaction{id=TxId, init_index_node={CoordPartition, Idx}, timestamp=Ts, ballots=Ballots},
             Timeout) ->
    Pool = maps:get(Idx, Pools),
    {ok, ReqId} = ext_shackle_transport:commit_request(Pool, TxId, CoordPartition, Ts, Ballots, Timeout),
    {ok, {commit, ReqId}}.

-spec await_commit(t(), commit_req_id()) -> ok | error.
await_commit(_, empty_commit) ->
    %% If the transaction never read anything, we bypass 2PC
    ok;

await_commit(_, {commit, ReqId}) ->
    case shackle:receive_response(ReqId) of
        {error, _} ->
            error;
        error ->
            error;
        ok ->
            ok
    end.

-spec commit(t(), tx()) -> ok | error.
commit(Coord, Tx) ->
    commit(Coord, Tx, infinity).

-spec commit_at(t(), tx(), index_node()) -> ok | error.
commit_at(_, #transaction{init_index_node=undefined}, _) ->
    ok;
commit_at(Coord, Tx, IndexNode) ->
    commit(Coord, Tx#transaction{init_index_node = IndexNode}, infinity).

-spec commit(t(), tx(), timeout()) -> ok | error.
commit(Coord, Tx, Timeout) ->
    {ok, Req} = async_commit(Coord, Tx, Timeout),
    await_commit(Coord, Req).

-spec release(t(), tx()) -> ok.
release(_, #transaction{init_index_node=undefined}) ->
    %% If the transaction never read anything, return
    ok;
release(#coordinator{conn_pool=Pools}, #transaction{id=TxId, init_index_node={_, Idx}, partitions=Partitions}) ->
    PartitionList = [ Partition || {Partition, _} <- maps:keys(Partitions) ],
    ext_shackle_transport:release(maps:get(Idx, Pools), TxId, PartitionList).

-spec async_read(t(), tx(), binary()) -> {ok, read_req_id()}.
async_read(#coordinator{ring=Ring, conn_pool=Pools},
           Tx=#transaction{timestamp=Ts, id=TxId, leaders=Leaders},
           Key) ->
    Idx = ext_ring:get_key_location(Ring, Key),
    Pool = maps:get(get_coord_node(Tx, Idx), Pools),
    {ok, ReqId} = ext_shackle_transport:read_request(Pool, maps:get(Idx, Leaders, empty), TxId, Ts, Key),
    {ok, {read, ReqId, Idx}}.

-spec await_read(t(), tx(), read_req_id()) -> {ok, binary(), tx()} | {error, tx()}.
await_read(_, Tx=#transaction{ballots=Ballots, leaders=Leaders, partitions=Partitions}, {read, ReqId, Idx={P, _}}) ->
    Tx0 = Tx#transaction{partitions=Partitions#{Idx => []}},
    case shackle:receive_response(ReqId) of
        error ->
            {error, Tx0};

        {ok, Ballot, ShardLeader, Value} ->
            Tx1 = Tx0#transaction{ballots=Ballots#{P => Ballot},
                                  leaders=Leaders#{Idx => ShardLeader}},

            {ok, Value, confirm_coord_index_node(Tx1, Idx)}
    end.

-spec async_update(t(), tx(), binary(), binary()) -> {ok, update_req_id()}.
async_update(#coordinator{ring=Ring, conn_pool=Pools},
             Tx=#transaction{timestamp=Ts, id=TxId, leaders=Leaders},
             Key,
             Value) ->
    Idx = ext_ring:get_key_location(Ring, Key),
    Pool = maps:get(get_coord_node(Tx, Idx), Pools),
    {ok, ReqId} = ext_shackle_transport:update_request(Pool, maps:get(Idx, Leaders, empty), TxId, Ts, Key, Value),
    {ok, {update, ReqId, Idx}}.

-spec await_update(t(), tx(), update_req_id()) -> {ok, tx()} | {error, tx()}.
await_update(_, Tx=#transaction{ballots=Ballots, leaders=Leaders, partitions=Partitions}, {update, ReqId, Idx={P, _}}) ->
    Tx0 = Tx#transaction{partitions=Partitions#{Idx => []}},
    case shackle:receive_response(ReqId) of
        error ->
            {error, Tx0};

        {ok, Ballot, ShardLeader} ->
            Tx1 = Tx0#transaction{ballots=Ballots#{P => Ballot},
                                  leaders=Leaders#{Idx => ShardLeader}},

            {ok, confirm_coord_index_node(Tx1, Idx)}
    end.

-spec sync_read(t(), tx(), binary()) -> {ok, binary(), tx()} | {error, tx()}.
sync_read(Coord, Tx, Key) ->
    {ok, Req} = async_read(Coord, Tx, Key),
    await_read(Coord, Tx, Req).

-spec sync_update(t(), tx(), binary(), binary()) -> {ok, tx()} | {error, tx()}.
sync_update(Coord, Tx, Key, Value) ->
    {ok, Req} = async_update(Coord, Tx, Key, Value),
    await_update(Coord, Tx, Req).

-spec ping(t(), tx(), binary()) -> ok.
ping(#coordinator{ring=Ring, conn_pool=Pools},
     Tx=#transaction{id=TxId},
     Key) ->
    Idx = ext_ring:get_key_location(Ring, Key),
    Pool = maps:get(get_coord_node(Tx, Idx), Pools),
    {ok, ReqId} = ext_shackle_transport:ping(Pool, TxId),
    shackle:receive_response(ReqId).

%%====================================================================
%% Internal functions
%%====================================================================

-spec get_coord_node(tx(), index_node()) -> node_and_port().
get_coord_node(#transaction{init_index_node=undefined}, {_, DefaultNode}) ->
    DefaultNode;

get_coord_node(#transaction{init_index_node={_, Node}}, _) ->
    Node.

-spec confirm_coord_index_node(tx(), index_node()) -> tx().
confirm_coord_index_node(Tx=#transaction{init_index_node=undefined}, IdxNode) -> Tx#transaction{init_index_node=IdxNode};
confirm_coord_index_node(Tx, _) -> Tx.

%%====================================================================
%% Util functions
%%====================================================================

-spec make_id_prefix(binary(), binary(), non_neg_integer()) -> binary().
make_id_prefix(ReplicaId, Ip, CoordId) ->
    <<
        ReplicaId/binary,
        "-",
        Ip/binary,
        "-",
        (integer_to_binary(CoordId))/binary,
        "-"
    >>.

-spec make_id_from_prefix(binary(), non_neg_integer()) -> binary().
make_id_from_prefix(Prefix, Id) ->
    <<Prefix/binary, (integer_to_binary(Id))/binary>>.
