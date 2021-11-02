-module(ext).
-include("ext.hrl").

%% Library lifetime management
-export([start/0,
         stop/0]).

%% Creation API
-export([default/4,
         new/5]).

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

%% Commit API
-export([commit/2]).

-record(coordinator, {
    %% The IP we're using to talk to the server
    %% Used to create a transaction id
    self_ip :: binary(),

    %% Routing info
    ring :: ext_ring:ext_ring(),

    %% Opened pool of connections, one pool per node in the cluster
    conn_pool :: #{{node_ip(), inet:port_number()} => shackle_pool()},

    tx_id_prefix :: binary()
}).

-record(transaction, {
    id :: binary(),
    timestamp :: timestamp(),
    init_node = undefined :: undefined | {node_ip(), inet:port_number()},
    leaders = #{} :: #{partition_id() => replica_id()},
    ballots = #{} :: #{partition_id() => ballot()}
}).

-opaque t() :: #coordinator{}.
-opaque tx() :: #transaction{}.

-opaque read_req_id() :: {read, shackle:external_request_id(), partition_id(), {node_ip(), inet:port_number()}}.
-opaque update_req_id() :: {update, shackle:external_request_id(), partition_id(), {node_ip(), inet:port_number()}}.

-export_type([t/0, tx/0, read_req_id/0, update_req_id/0]).

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
          NodePool :: #{{node_ip(), inet:port_number()} => shackle_pool()}) -> {ok, t()}.

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

-spec commit(t(), tx()) -> ok | error.
commit(_, #transaction{init_node=undefined}) ->
    %% If the transaction never read anything, we bypass 2PC
    ok;
commit(#coordinator{conn_pool=Pools}, #transaction{id=TxId, init_node=Idx, ballots=Ballots}) ->
    Pool = maps:get(Idx, Pools),
    ext_shackle_transport:commit(Pool, TxId, Ballots).

-spec async_read(t(), tx(), binary()) -> {ok, read_req_id()}.
async_read(#coordinator{ring=Ring, conn_pool=Pools},
           #transaction{timestamp=Ts, id=TxId, leaders=Leaders},
           Key) ->
    {P, Idx} = ext_ring:get_key_location(Ring, Key),
    Pool = maps:get(Idx, Pools),
    {ok, ReqId} = ext_shackle_transport:read_request(Pool, maps:get(P, Leaders, empty), TxId, Ts, Key),
    {ok, {read, ReqId, P, Idx}}.

-spec await_read(t(), tx(), read_req_id()) -> {ok, binary(), tx()} | error.
await_read(_, Tx=#transaction{ballots=Ballots}, {read, ReqId, P, Idx}) ->
    case shackle:receive_response(ReqId) of
        error ->
            error;
        {ok, Ballot, ShardLeader, Value} ->
            {
                ok,
                Value,
                set_tx_init_node(Tx#transaction{ballots=Ballots#{P => Ballot}, leaders=#{P => ShardLeader}}, Idx)
            }
    end.

-spec async_update(t(), tx(), binary(), binary()) -> {ok, update_req_id()}.
async_update(#coordinator{ring=Ring, conn_pool=Pools},
             #transaction{timestamp=Ts, id=TxId, leaders=Leaders},
             Key,
             Value) ->
    {P, Idx} = ext_ring:get_key_location(Ring, Key),
    Pool = maps:get(Idx, Pools),
    {ok, ReqId} = ext_shackle_transport:update_request(Pool, maps:get(P, Leaders, empty), TxId, Ts, Key, Value),
    {ok, {update, ReqId, P, Idx}}.

-spec await_update(t(), tx(), update_req_id()) -> {ok, tx()} | error.
await_update(_, Tx=#transaction{ballots=Ballots}, {update, ReqId, P, Idx}) ->
    case shackle:receive_response(ReqId) of
        error ->
            error;
        {ok, Ballot, ShardLeader} ->
            {
                ok,
                set_tx_init_node(Tx#transaction{ballots=Ballots#{P => Ballot}, leaders=#{P => ShardLeader}}, Idx)
            }
    end.

-spec sync_read(t(), tx(), binary()) -> {ok, binary(), tx()} | error.
sync_read(Coord, Tx, Key) ->
    {ok, Req} = async_read(Coord, Tx, Key),
    await_read(Coord, Tx, Req).

-spec sync_update(t(), tx(), binary(), binary()) -> {ok, tx()} | error.
sync_update(Coord, Tx, Key, Value) ->
    {ok, Req} = async_update(Coord, Tx, Key, Value),
    await_update(Coord, Tx, Req).

%%====================================================================
%% Internal functions
%%====================================================================

-spec set_tx_init_node(tx(), {node_ip(), inet:port_number()}) -> tx().
set_tx_init_node(Tx=#transaction{init_node=undefined}, Idx) -> Tx#transaction{init_node=Idx};
set_tx_init_node(Tx, _) -> Tx.

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
