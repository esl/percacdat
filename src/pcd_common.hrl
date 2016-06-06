-define(PCD_DEFAULT_DB_MODULE, pcd_db_riak).
-define(PCD_DEFAULT_CHUNK_SIZE, 500).

-record(chunk_key,
        {
            id              = <<"">>                    :: binary(),
            chunk_nr                                    :: undefined | non_neg_integer()
        }).

-record(row_param,
        {
            index                                       :: term(),
            params                                      :: term()
        }).

-type row_param() :: #row_param{}.
