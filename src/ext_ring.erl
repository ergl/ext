-module(ext_ring).
-include("ext.hrl").

%% @doc Fixed ring structured used to route protocol requests
%%
%%      Uses a tuple-based structure to enable index accesses
%%      in constant time.
%%
-type ext_fixed_ring() :: tuple().
-record(ext_ring, {
    size :: non_neg_integer(),
    fixed_ring :: ext_fixed_ring()
}).

-opaque t() :: #ext_ring{}.
-export_type([t/0]).

%% API
-export([replica_info/3,
         random_partition/1,
         get_key_location/2,
         size/1]).

-spec replica_info(
    MyReplica :: replica_id(),
    Address :: node_ip(),
    Port :: inet:port_number()
) -> {ok, LocalIp :: inet:ip_address(), Ring :: t(), LocalNodes :: [{partition_id(), node_ip(), inet:port_number()}]}
    | {error, inet:posix()}.
replica_info(MyReplica, Address, Port) ->
    ConnOpts = [binary, {active, false}, {packet, 4}, {nodelay, true}],
    case gen_tcp:connect(Address, Port, ConnOpts) of
        {error, Reason} ->
            {error, Reason};
        {ok, Sock} ->
            {ok, {LocalIp, _}} = inet:sockname(Sock),
            Request = #{seq => 0, payload => {getReplicaNodes, #{forReplica => MyReplica}}},
            Msg = ext_master_proto:encode_msg(Request, 'master.Request'),
            ok = gen_tcp:send(Sock, Msg),
            Reply =
                case gen_tcp:recv(Sock, 0) of
                    {error, Reason} ->
                        {error, Reason};
                    {ok, RawReply} ->
                        #{payload := {getReplicaNodesReply, #{nodes := Addrs}}} =
                            ext_master_proto:decode_msg(RawReply, 'master.Response'),
                        Nodes = transform_addrs(Addrs),
                        {ok, LocalIp, make_ring(Nodes), Nodes}
                end,
            gen_tcp:close(Sock),
            Reply
    end.

-spec random_partition(t()) -> index_node().
random_partition(#ext_ring{size=Size, fixed_ring=Layout}) ->
    erlang:element(rand:uniform(Size), Layout).

-spec get_key_location(t(), binary()) -> index_node().
get_key_location(#ext_ring{size=Size, fixed_ring=Layout}, Key) ->
    Pos = convert_key(Key) rem Size + 1,
    erlang:element(Pos, Layout).

-spec size(t()) -> non_neg_integer().
size(#ext_ring{size=Size}) -> Size.

%%====================================================================
%% Routing Internal functions
%%====================================================================

-spec convert_key(binary()) -> non_neg_integer().
convert_key(Key) ->
    convert_key_binary(Key).

-spec convert_key_binary(binary()) -> non_neg_integer().
convert_key_binary(<<X:64>>) ->
    convert_key_int(X);
convert_key_binary(<<X:32>>) ->
    convert_key_int(X);
convert_key_binary(Bin) ->
    case catch binary_to_integer(Bin) of
        N when is_integer(N) ->
            convert_key_int(N);
        _ ->
            convert_key_hash(Bin)
    end.

-spec convert_key_int(integer()) -> non_neg_integer().
convert_key_int(Int) ->
    abs(Int).

-spec convert_key_hash(term()) -> non_neg_integer().
convert_key_hash(Key) ->
    HashKey = crypto:hash(sha, term_to_binary(Key)),
    abs(crypto:bytes_to_integer(HashKey)).

%%====================================================================
%% Partition Internal functions
%%====================================================================

-spec transform_addrs(#{partition_id() => #{ip := unicode:chardata(), port := non_neg_integer()}}) -> [{partition_id(), node_ip(), inet:port_number()}].
transform_addrs(Addrs) ->
    maps:fold(
        fun(P, #{ip := IP, port := Port}, Acc) ->
            {ok, Addr} = inet:getaddr(binary_to_list(IP), inet),
            [ {P, inet:ntoa(Addr), Port} | Acc ]
        end,
        [],
        Addrs
    ).

-spec make_ring([{partition_id(), node_ip(), inet:port_number()}]) -> t().
make_ring(Nodes) ->
    Size = erlang:length(Nodes),
    #ext_ring{
        size=Size,
        fixed_ring=make_fixed_ring(Size, Nodes)
    }.

%% @doc Convert a raw riak ring into a fixed tuple structure
-spec make_fixed_ring(non_neg_integer(), [{partition_id(), node_ip(), inet:port_number()}]) -> ext_fixed_ring().
make_fixed_ring(Size, Nodes) ->
    erlang:make_tuple(Size, [], index_ring(Nodes)).

-spec index_ring([{partition_id(), node_ip(), inet:port_number()}]) -> [{non_neg_integer(), {partition_id(), inet:ip_address(), inet:port_number()}}].
index_ring(Nodes) ->
    %% Partitions are zero-based, so add 1
    [ {P + 1, {P, {IP, Port}}} || {P, IP, Port} <- Nodes].
