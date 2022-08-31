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
         sync_update/3,
         sync_update/4,
         sync_operation/4]).

%% Async read / update API
-export([async_read/3,
         await_read/3,
         async_operation/4,
         await_operation/3,
         async_multi_read/3,
         async_multi_update/3]).

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
    leaders = #{} :: #{partition_id() => replica_id()},
    partitions = #{} :: #{partition_id() => []}
}).

-opaque t() :: #coordinator{}.
-opaque tx() :: #transaction{}.

-type read_batch_pieces() :: #{partition_id() := ext_client_proto:'client.ReadBatch.Piece'()}.
-type update_batch_pieces() :: #{partition_id() := ext_client_proto:'client.UpdateBatch.Piece'()}.

-type key_pos_index() :: #{ {partition_id(), non_neg_integer()} => binary() }.
-type read_batch_req_id() :: {
    read_batch,
    shackle:external_request_id(),
    index_node(),
    key_pos_index(),
    #{partition_id() => []}
}.


-opaque read_req_id() :: {read, shackle:external_request_id(), index_node()}
                       | read_batch_req_id().
-opaque update_req_id() :: {update, shackle:external_request_id(), index_node()}.
-opaque update_batch_req_id() :: {
    update_batch,
    shackle:external_request_id(),
    index_node(),
    #{partition_id() => []}
}.
-opaque commit_req_id() :: {commit, shackle:external_request_id()} | empty_commit.

-export_type([t/0, tx/0, read_req_id/0, update_req_id/0, update_batch_req_id/0, commit_req_id/0]).

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

async_commit(
    #coordinator{conn_pool=Pools},
    #transaction{id=TxId, init_index_node={CoordPartition, Idx}, timestamp=Ts, ballots=Ballots},
    Timeout
) ->
    Pool = maps:get(Idx, Pools),
    {ok, ReqId} = ext_shackle_transport:commit_request(
        Pool,
        TxId,
        CoordPartition,
        Ts,
        Ballots,
        Timeout
    ),
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
release(
    #coordinator{conn_pool=Pools},
    #transaction{id=TxId, init_index_node={_, Idx},
    partitions=Partitions
}) ->
    ext_shackle_transport:release(maps:get(Idx, Pools), TxId, maps:keys(Partitions)).

-spec async_read(t(), tx(), binary() | [binary(), ...]) -> {ok, read_req_id()}.
async_read(Coord, Tx, Keys) when is_list(Keys) ->
    case Keys of
        [ Key ] ->
            async_read(Coord, Tx, Key);
        [ _ | _ ] ->
            async_multi_read(Coord, Tx, Keys)
    end;

async_read(
    #coordinator{ring=Ring, conn_pool=Pools},
    Tx=#transaction{timestamp=Ts, id=TxId, leaders=Leaders},
    Key
) ->
    {Partition, _}=Idx = ext_ring:get_key_location(Ring, Key),
    LeaderId = maps:get(Partition, Leaders, empty),
    Pool = maps:get(get_coord_node(Tx, Idx), Pools),
    {ok, ReqId} = ext_shackle_transport:read_request(Pool, LeaderId, TxId, Ts, Key),
    {ok, {read, ReqId, Idx}}.

-spec await_read(Coordinator, Tx0, ReadReq) -> {ok, Value, Tx}
                                             | {ok, ValueMap, Tx}
                                             | {error, Tx} when
    Coordinator :: t(),
    Tx0 :: tx(),
    ReadReq :: read_req_id(),
    Value :: binary(),
    ValueMap :: #{binary() => binary()},
    Tx :: tx().

await_read(
    _,
    Tx=#transaction{ballots=Ballots, leaders=Leaders, partitions=Partitions},
    {read, ReqId, Idx={P, _}}
) ->
    Tx0 = Tx#transaction{partitions=Partitions#{P => []}},
    case shackle:receive_response(ReqId) of
        error ->
            {error, Tx0};

        {ok, Ballot, ShardLeader, Value} ->
            Tx1 = Tx0#transaction{ballots=Ballots#{P => Ballot},
                                  leaders=Leaders#{P => ShardLeader}},

            {ok, Value, confirm_coord_index_node(Tx1, Idx)}
    end;

await_read(
    _,
    Tx=#transaction{partitions=Partitions},
    {read_batch, ReqId, Idx, KeyPosIndex, PendingPartitions}
) ->

    Tx0 = Tx#transaction{partitions=maps:merge(Partitions, PendingPartitions)},
    case shackle:receive_response(ReqId) of
        error ->
            {error, Tx0};

        {ok, Payload} ->
            {Tx1, ValueMap} =
                maps:fold(
                    fun(
                        Partition,
                        #{ballot := Ballot, servedBy := ShardLeader, values := Values},
                        {TxAcc, ValueAcc}
                    ) ->
                        {
                            TxAcc#transaction{
                                ballots = (TxAcc#transaction.ballots)#{Partition => Ballot},
                                leaders = (TxAcc#transaction.leaders)#{Partition => ShardLeader}
                            },
                            fill_from_key_index(Partition, Values, KeyPosIndex, ValueAcc)
                        }
                    end,
                    {Tx0, #{}},
                    Payload
                ),
            {ok, ValueMap, confirm_coord_index_node(Tx1, Idx)}
    end.

-spec async_operation(t(), tx(), binary(), operation()) -> {ok, update_req_id()}.
async_operation(
    #coordinator{ring=Ring, conn_pool=Pools},
    Tx=#transaction{timestamp=Ts, id=TxId, leaders=Leaders},
    Key,
    Operation
) ->
    {Partition, _}=Idx = ext_ring:get_key_location(Ring, Key),
    LeaderId = maps:get(Partition, Leaders, empty),
    Pool = maps:get(get_coord_node(Tx, Idx), Pools),
    {ok, ReqId} = ext_shackle_transport:update_request(Pool, LeaderId, TxId, Ts, Key, Operation),
    {ok, {update, ReqId, Idx}}.

-spec await_operation(t(), tx(), update_req_id()) -> {ok, tx()} | {error, tx()}.
await_operation(
    _,
    Tx=#transaction{ballots=Ballots, leaders=Leaders, partitions=Partitions},
    {update, ReqId, Idx={P, _}}
) ->
    Tx0 = Tx#transaction{partitions=Partitions#{P => []}},
    case shackle:receive_response(ReqId) of
        error ->
            {error, Tx0};

        {ok, Ballot, ShardLeader} ->
            Tx1 = Tx0#transaction{ballots=Ballots#{P => Ballot},
                                  leaders=Leaders#{P => ShardLeader}},

            {ok, confirm_coord_index_node(Tx1, Idx)}
    end.

async_multi_read(
    #coordinator{ring=Ring, conn_pool=Pools},
    Tx=#transaction{id=TxId, timestamp=Ts, leaders=Leaders},
    Keys=[HeadKey | _]
) ->
    {Pieces, Pending} = assemble_read_pieces(Ring, Leaders, Keys),
    KeyIndex = build_key_index(Pieces),
    CoordIdx = ext_ring:get_key_location(Ring, HeadKey),
    CoordPool = maps:get(get_coord_node(Tx, CoordIdx), Pools),
    {ok, ReqId} = ext_shackle_transport:read_batch_request(CoordPool, TxId, Ts, Pieces),
    {ok, {read_batch, ReqId, CoordIdx, KeyIndex, Pending}}.

-spec async_multi_update(Coordinator, TxId, Updates) -> {ok, ReqId} when
    Coordinator :: t(),
    TxId :: tx(),
    Updates :: [{binary(), binary()}],
    ReqId :: update_batch_req_id().

async_multi_update(
    #coordinator{ring=Ring, conn_pool=Pools},
    Tx=#transaction{id=TxId, timestamp=Ts, leaders=Leaders},
    Updates = [ {HeadKey, _} | _ ]
) when is_list(Updates) ->
    {Pieces, Pending} = assemble_update_pieces(Ring, Leaders, Updates),
    CoordIdx = ext_ring:get_key_location(Ring, HeadKey),
    CoordPool = maps:get(get_coord_node(Tx, CoordIdx), Pools),
    {ok, ReqId} = ext_shackle_transport:update_batch_request(CoordPool, TxId, Ts, Pieces),
    {ok, {update_batch, ReqId, CoordIdx, Pending}}.

-spec await_multi_update(Coordinator, tx(), ReqId) -> {ok, tx()} | {error, tx()} when
    Coordinator :: t(),
    ReqId :: update_batch_req_id().

await_multi_update(
    _,
    Tx=#transaction{partitions=Partitions},
    {update_batch, ReqId, Idx, PendingPartitions}
) ->
    Tx0 = Tx#transaction{partitions=maps:merge(Partitions, PendingPartitions)},
    case shackle:receive_response(ReqId) of
        error ->
            {error, Tx0};

        {ok, Payload} ->
            Tx1 =
                maps:fold(
                    fun(
                        Partition,
                        #{ballot := Ballot, servedBy := ShardLeader},
                        TxAcc
                    ) ->
                        TxAcc#transaction{
                            ballots = (TxAcc#transaction.ballots)#{Partition => Ballot},
                            leaders = (TxAcc#transaction.leaders)#{Partition => ShardLeader}
                        }
                    end,
                    Tx0,
                    Payload
                ),
            {ok, confirm_coord_index_node(Tx1, Idx)}
    end.

-spec sync_read(t(), tx(), binary()) -> {ok, binary(), tx()} | {error, tx()}.
sync_read(Coord, Tx, Key) ->
    {ok, Req} = async_read(Coord, Tx, Key),
    await_read(Coord, Tx, Req).

-spec sync_update(t(), tx(), [{binary(), binary()}]) -> {ok, tx()} | {error, tx()}.
sync_update(Coord, Tx, Updates) ->
    {ok, Req} = async_multi_update(Coord, Tx, Updates),
    await_multi_update(Coord, Tx, Req).

-spec sync_update(t(), tx(), binary(), binary()) -> {ok, tx()} | {error, tx()}.
sync_update(Coord, Tx, Key, Value) ->
    sync_operation(Coord, Tx, Key, {data, Value}).

-spec sync_operation(t(), tx(), binary(), operation()) -> {ok, tx()} | {error, tx()}.
sync_operation(Coord, Tx, Key, Op) ->
    {ok, Req} = async_operation(Coord, Tx, Key, Op),
    await_operation(Coord, Tx, Req).

-spec ping(t(), tx(), binary()) -> ok.
ping(
    #coordinator{ring=Ring, conn_pool=Pools},
    Tx=#transaction{id=TxId},
    Key
) ->
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
%% Read Batch Util
%%====================================================================

-spec assemble_read_pieces(Ring, Leaders, Keys) -> {Pieces, PendingPartitions} when
    Ring :: ext_ring:ext_ring(),
    Leaders :: #{partition_id() => replica_id()},
    Keys :: [binary(), ...],
    Pieces :: read_batch_pieces(),
    PendingPartitions :: #{partition_id() => []}.

assemble_read_pieces(Ring, Leaders, Keys) ->
    assemble_read_pieces(Ring, Leaders, Keys, #{}, #{}).

-spec assemble_read_pieces(Ring, Leaders, Keys, PieceAcc, PendingAcc) -> {Pieces, PendingPartitions} when
    Ring :: ext_ring:ext_ring(),
    Leaders :: #{partition_id() => replica_id()},
    Keys :: [binary(), ...],
    PieceAcc :: read_batch_pieces(),
    PendingAcc :: #{partition_id() => []},
    Pieces :: read_batch_pieces(),
    PendingPartitions :: #{partition_id() => []}.

assemble_read_pieces(_Ring, _Leaders, [], PieceAcc, PendingAcc) ->
    {PieceAcc, PendingAcc};

assemble_read_pieces(Ring, Leaders, [Key | Rest], Acc0, PendingAcc) ->
    {Partition, _} = ext_ring:get_key_location(Ring, Key),
    Acc =
        case Acc0 of
            #{Partition := M=#{keys := Keys}} ->
                Acc0#{Partition => M#{
                    keys => Keys ++ [Key]
                }};

            _ ->
                PartitionMap =
                    case Leaders of
                        #{Partition := LeaderId} ->
                            #{prevLeader => LeaderId, keys => [Key]};
                        _ ->
                            #{keys => [Key]}
                    end,
                Acc0#{Partition => PartitionMap}
        end,
    assemble_read_pieces(Ring, Leaders, Rest, Acc, PendingAcc#{Partition => []}).

-spec build_key_index(read_batch_pieces()) -> key_pos_index().
build_key_index(PartitionMap) ->
    maps:fold(
        fun(Partition, #{keys := KeyList}, Acc) ->
            build_partition_index(Partition, KeyList, Acc)
        end,
        #{},
        PartitionMap
    ).

-spec build_partition_index(
    Partition :: partition_id(),
    Keys :: [binary(), ...],
    Acc :: key_pos_index()
) -> key_pos_index().
build_partition_index(Partition, Keys, Acc) ->
    {Idx, _} =
        lists:foldl(
            fun(Key, {InnerAcc, N}) ->
                {
                    InnerAcc#{{Partition, N} => Key},
                    N + 1
                }
            end,
            {Acc, 0},
            Keys
        ),
    Idx.

-spec fill_from_key_index(Partition, Values, Index, Acc0) -> Acc when
    Partition :: partition_id(),
    Values :: [binary(), ...],
    Index :: key_pos_index(),
    Acc0 :: #{binary() => binary()},
    Acc :: #{binary() => binary()}.

fill_from_key_index(Partition, Values, Index, Acc0) ->
    {ValueMap, _} =
        lists:foldl(
            fun(Value, {Acc, N}) ->
                Key = maps:get({Partition, N}, Index),
                {
                    Acc#{Key => Value},
                    N+1
                }
            end,
            {Acc0, 0},
            Values
        ),
    ValueMap.

%%====================================================================
%% Update Batch Util
%%====================================================================

-spec assemble_update_pieces(Ring, Leaders, Updates) -> {Pieces, PendingPartitions} when
    Ring :: ext_ring:ext_ring(),
    Leaders :: #{partition_id() => replica_id()},
    Updates :: [{binary(), binary()}],
    Pieces :: update_batch_pieces(),
    PendingPartitions :: #{partition_id() => []}.

assemble_update_pieces(Ring, Leaders, Updates) ->
    assemble_update_pieces(Ring, Leaders, Updates, #{}, #{}).

-spec assemble_update_pieces(Ring, Leaders, Updates, PieceAcc, PendingAcc) -> {Pieces, PendingPartitions} when
    Ring :: ext_ring:ext_ring(),
    Leaders :: #{partition_id() => replica_id()},
    Updates :: [{binary(), binary()}],
    PieceAcc :: update_batch_pieces(),
    PendingAcc :: #{partition_id() => []},
    Pieces :: update_batch_pieces(),
    PendingPartitions :: #{partition_id() => []}.

assemble_update_pieces(_Ring, _Leaders, [], PieceAcc, PendingAcc) ->
    {PieceAcc, PendingAcc};

assemble_update_pieces(Ring, Leaders, [{Key, Value} | Rest], Acc0, PendingAcc) ->
    {Partition, _} = ext_ring:get_key_location(Ring, Key),
    Acc =
        case Acc0 of
            #{Partition := M=#{updates := Keys}} ->
                Acc0#{Partition => M#{
                    updates => Keys ++ [#{key => Key, data => Value}]
                }};

            _ ->
                PartitionMap =
                    case Leaders of
                        #{Partition := LeaderId} ->
                            #{prevLeader => LeaderId, keys => [Key]};
                        _ ->
                            #{updates => [#{key => Key, data => Value}]}
                    end,
                Acc0#{Partition => PartitionMap}
        end,
    assemble_update_pieces(Ring, Leaders, Rest, Acc, PendingAcc#{Partition => []}).

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
