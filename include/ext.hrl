-type node_ip() :: atom() | inet:ip_address().
-type partition_id() :: non_neg_integer().
-type replica_id() :: binary().
-type shackle_pool() :: atom().
-type timestamp() :: non_neg_integer().
-type ballot() :: non_neg_integer().
-type node_and_port() :: {node_ip(), inet:port_number()}.
-type index_node() :: {partition_id(), node_and_port()}.

-export_type([node_ip/0,
              node_and_port/0,
              partition_id/0,
              index_node/0,
              replica_id/0,
              shackle_pool/0,
              timestamp/0,
              ballot/0]).