-module(ext).
-include("ext.hrl").

%% Library lifetime management
-export([start/0,
         stop/0]).

%% Creation API
-export([new/5]).

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
    %% Replica ID of the connected cluster
    replica_id :: replica_id(),

    %% Opened pool of connections, one pool per node in the cluster
    conn_pool :: #{{node_ip(), inet:port_number()} => shackle_pool()},

    coordinator_id :: non_neg_integer()
}).

-record(transaction, {
    id :: binary(),
    timestamp :: timestamp(),
    threshold_timestamp :: timestamp(),
    init_node :: {node_ip(), inet:port_number()},
    leaders = #{} :: #{partition_id() => replica_id()},
    ballots = #{} :: #{partition_id() => ballot()}
}).

-opaque t() :: #coordinator{}.
-opaque tx() :: #transaction{}.

-opaque read_req_id() :: {read, shackle:external_request_id(), partition_id()}.
-opaque update_req_id() :: {update, shackle:external_request_id(), partition_id()}.

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

-spec new(ReplicaId :: replica_id(),
          LocalIP :: node_ip(),
          WorkerId :: non_neg_integer(),
          RingInfo :: ext_ring:t(),
          NodePool :: #{{node_ip(), inet:port_number()} => shackle_pool()}) -> {ok, t()}.

new(ReplicaId, LocalIP, WorkerId, RingInfo, NodePool) ->
    {ok, #coordinator{self_ip=list_to_binary(inet:ntoa(LocalIP)),
                      ring=RingInfo,
                      replica_id=ReplicaId,
                      conn_pool=NodePool,
                      coordinator_id=WorkerId}}.

%%====================================================================
%% Client API functions
%%====================================================================

-spec start_transaction(t(), non_neg_integer()) -> {ok, tx()}.
start_transaction(#coordinator{self_ip=Ip, coordinator_id=LocalId, replica_id=ReplicaId, ring=Ring}, Id) ->
    TxId = make_id(ReplicaId, Ip, LocalId, Id),
    Ts0 = erlang:system_time(nanosecond),
    %% FIXME(borja, time): Clock skew?
    Ts1 = Ts0,
    {_, Idx} = ext_ring:random_partition(Ring),
    {ok, #transaction{id=TxId, init_node=Idx, timestamp=Ts0, threshold_timestamp=Ts1}}.

-spec commit(t(), tx()) -> ok | error.
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
    {ok, {read, ReqId, P}}.

-spec await_read(t(), tx(), read_req_id()) -> {ok, binary(), tx()} | error.
await_read(_, Tx=#transaction{ballots=Ballots}, {read, ReqId, P}) ->
    case shackle:receive_response(ReqId) of
        error ->
            error;
        {ok, Ballot, ShardLeader, Value} ->
            {ok, Value, Tx#transaction{ballots=Ballots#{P => Ballot}, leaders=#{P => ShardLeader}}}
    end.

-spec async_update(t(), tx(), binary(), binary()) -> {ok, update_req_id()}.
async_update(#coordinator{ring=Ring, conn_pool=Pools},
             #transaction{timestamp=Ts, id=TxId, leaders=Leaders},
             Key,
             Value) ->
    {P, Idx} = ext_ring:get_key_location(Ring, Key),
    Pool = maps:get(Idx, Pools),
    {ok, ReqId} = ext_shackle_transport:update_request(Pool, maps:get(P, Leaders, empty), TxId, Ts, Key, Value),
    {ok, {update, ReqId, P}}.

-spec await_update(t(), tx(), update_req_id()) -> {ok, tx()} | error.
await_update(_, Tx=#transaction{ballots=Ballots}, {update, ReqId, P}) ->
    case shackle:receive_response(ReqId) of
        error ->
            error;
        {ok, Ballot, ShardLeader} ->
            {ok, Tx#transaction{ballots=Ballots#{P => Ballot}, leaders=#{P => ShardLeader}}}
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

%%====================================================================
%% Util functions
%%====================================================================

%% TODO(borja): Rethink
-spec make_id(binary(), binary(), non_neg_integer(), non_neg_integer()) -> binary().
make_id(ReplicaId, Ip, LocalId, Id) ->
    <<
        ReplicaId/binary,
        Ip/binary,
        (integer_to_binary(LocalId))/binary,
        (integer_to_binary(Id))/binary
    >>.
