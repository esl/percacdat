-ifndef(PCD_COMMON).
-define(PCD_COMMON, pcd).

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
-endif.