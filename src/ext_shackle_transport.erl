-module(ext_shackle_transport).
-include("ext.hrl").
-behavior(shackle_client).

-export([read_request/5,
         read_batch_request/4,
         update_request/6,
         update_batch_request/4,
         commit_request/6,
         release/3]).

-export([ping/2]).

%% API
-export([init/1,
         setup/2,
         handle_request/2,
         handle_data/2,
         handle_timeout/2,
         terminate/1]).

-record(state, {
    req_counter :: non_neg_integer(),
    max_req_id :: non_neg_integer()
}).
-type state() :: #state{}.

ping(Pool, TxId) ->
    shackle:cast(Pool, {ping, TxId}, self(), infinity).

-spec read_request(
    Pool :: shackle_pool(),
    PrevLeader :: replica_id() | empty,
    TxId :: binary(),
    Timestamp :: timestamp(),
    Key :: binary()
) -> {ok, shackle:external_request_id()}.
%% When awaited, returns {ok, Ballot :: ballot(), ServedBy :: replica_id(), Data :: binary()} | error.
read_request(Pool, PrevLeader, TxId, Timestamp, Key) ->
    shackle:cast(Pool, {read, PrevLeader, TxId, Timestamp, Key}, self(), infinity).

-spec read_batch_request(
    Pool :: shackle_pool(),
    TxId :: binary(),
    Timestamp :: timestamp(),
    Pieces :: ext:read_batch_pieces()
) -> {ok, shackle:external_request_id()}.
%% When awaited, returns {ok, #{partition_id() => 'client.ReadBatchReply.Piece'}} | error.
read_batch_request(Pool, TxId, Timestamp, Pieces) ->
    shackle:cast(Pool, {read_batch, TxId, Timestamp, Pieces}, self(), infinity).

-spec update_request(
    Pool :: shackle_pool(),
    PrevLeader :: replica_id() | empty,
    TxId :: binary(),
    Timestamp :: timestamp(),
    Key :: binary(),
    Operation :: operation()
) -> {ok, shackle:external_request_id()}.
%% When awaited, returns {ok, Ballot :: ballot(), ServedBy :: replica_id()} | error.
update_request(Pool, PrevLeader, TxId, Timestamp, Key, Operation) ->
    shackle:cast(Pool, {update, PrevLeader, TxId, Timestamp, Key, Operation}, self(), infinity).

-spec update_batch_request(Pool, TxId, Timestamp, Pieces) -> {ok, ReqId} when
    Pool :: shackle_pool(),
    TxId :: binary(),
    Timestamp :: timestamp(),
    Pieces :: ext:update_batch_pieces(),
    ReqId :: shackle:external_request_id().

update_batch_request(Pool, TxId, Timestamp, Pieces) ->
    shackle:cast(Pool, {update_batch, TxId, Timestamp, Pieces}, self(), infinity).

-spec commit_request(
    Pool :: shackle_pool(),
    TxId :: binary(),
    CoordPartition :: partition_id(),
    Timestamp :: timestamp(),
    Ballots :: #{partition_id() => ballot()},
    Timeout :: timeout()
) -> {ok, shackle:external_request_id()}.
%% When awaited, returns ok | error.
commit_request(Pool, TxId, CoordPartition, Timestamp, Ballots, Timeout) ->
    shackle:cast(Pool, {commit, TxId, CoordPartition, Timestamp, Ballots}, self(), Timeout).

-spec release(shackle_pool(), binary(), [partition_id()]) -> ok.
release(Pool, TxId, Partitions) ->
    shackle:call(Pool, {release, TxId, Partitions}, infinity).

%%%===================================================================
%%% shackle callbacks
%%%===================================================================

init(_Options) ->
    MaxId = trunc(math:pow(2, 32)),
    {ok, #state{req_counter=0, max_req_id=MaxId}}.

setup(_Socket, State) ->
    {ok, State}.

handle_request({ping, TxId}, S=#state{req_counter=Req}) ->
    Msg = #{txId => TxId},
    {ok, Req, make_request(Req, {ping, Msg}), incr_req(S)};

handle_request({read, PrevLeader, TxId, Timestamp, Key}, S=#state{req_counter=Req}) ->
    Msg0 = #{txId => TxId, timestamp => Timestamp, key => Key},
    Msg = case PrevLeader of empty -> Msg0; _ -> Msg0#{prevLeader => PrevLeader} end,
    {ok, Req, make_request(Req, {read, Msg}), incr_req(S)};

handle_request({read_batch, TxId, Timestamp, Pieces}, S=#state{req_counter=Req}) ->
    Msg = #{txId => TxId, timestamp => Timestamp, pieces => Pieces},
    {ok, Req, make_request(Req, {readBatch, Msg}), incr_req(S)};

handle_request({update, PrevLeader, TxId, Timestamp, Key, Operation}, S=#state{req_counter=Req}) ->
    Msg0 = #{txId => TxId, timestamp => Timestamp, key => Key, operation => Operation},
    Msg = case PrevLeader of empty -> Msg0; _ -> Msg0#{prevLeader => PrevLeader} end,
    {ok, Req, make_request(Req, {update, Msg}), incr_req(S)};

handle_request({update_batch, TxId, Timestamp, Pieces}, S=#state{req_counter=Req}) ->
    Msg = #{txId => TxId, timestamp => Timestamp, pieces => Pieces},
    {ok, Req, make_request(Req, {updateBatch, Msg}), incr_req(S)};

handle_request({commit, TxId, CoordPartition, Timestamp, Ballots}, S=#state{req_counter=Req}) ->
    Msg = #{txId => TxId, timestamp => Timestamp, ballots => Ballots, coordPartition => CoordPartition},
    {ok, Req, make_request(Req, {commit, Msg}), incr_req(S)};

handle_request({release, TxId, Partitions}, S=#state{req_counter=Req}) ->
    Msg = #{txId => TxId, partitions => Partitions},
    %% Req = undefined means shackle won't wait for a reply
    {ok, undefined, make_request(Req, {release, Msg}), incr_req(S)};

handle_request(_Request, _State) ->
    erlang:error(unknown_request).

handle_data(Data, State) ->
    {ok, [decode_reply(Data)], State}.

handle_timeout(RequestId, State) ->
    {ok, {RequestId, {error, timeout}}, State}.

terminate(_State) -> ok.

%% Util

-spec make_request(non_neg_integer(), {atom(), #{_ => _}}) -> binary().
make_request(Req, Payload) ->
    ext_client_proto:encode_msg(#{seq => Req, payload => Payload}, 'client.Request').

-spec decode_reply(binary()) -> {non_neg_integer(), term()}.
decode_reply(Payload) ->
    #{seq := Seq, payload := InnerPayload} = ext_client_proto:decode_msg(Payload, 'client.Response'),
    {Seq, decode_payload(InnerPayload)}.

-spec decode_payload({read | update | commit, _}) -> term().
decode_payload({read, Map}) ->
    case Map of
        #{ballot := B, servedBy := L, data := D, isError := false} -> {ok, B, L, D};
        _ -> error
    end;
decode_payload({update, Map}) ->
    case Map of
        #{ballot := B, servedBy := L, isError := false} -> {ok, B, L};
        _ -> error
    end;
decode_payload(({readBatch, Map})) ->
    case Map of
        #{payload := Payload} when map_size(Payload) =/= 0 ->
            {ok, Payload};
        _ ->
            error
    end;
decode_payload(({updateBatch, Map})) ->
    case Map of
        #{payload := Payload} when map_size(Payload) =/= 0 ->
            {ok, Payload};
        _ ->
            error
    end;
decode_payload({commit, #{commit := true}}) -> ok;
decode_payload({commit, #{commit := false}}) -> error;
decode_payload({pong, _}) -> ok.

-spec incr_req(state()) -> state().
incr_req(S=#state{req_counter=ReqC, max_req_id=MaxId}) ->
    S#state{req_counter=((ReqC + 1) rem MaxId)}.
