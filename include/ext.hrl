-type node_ip() :: atom() | inet:ip_address().
-type partition_id() :: non_neg_integer().
-type replica_id() :: binary().
-type shackle_pool() :: atom().
-type timestamp() :: non_neg_integer().
-type ballot() :: non_neg_integer().

-export_type([node_ip/0,
              partition_id/0,
              replica_id/0,
              shackle_pool/0,
              timestamp/0,
              ballot/0]).