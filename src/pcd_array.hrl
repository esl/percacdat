-include("pcd_common.hrl").
-define(PCD_DEFAULT_ROW_SIZE, 500).
-define(PCD_ARRAYS_PATH(Service), << (Service)/binary,
                                   "-",
                                   "arrays" >>).
-define(PCD_ARRAYS_BUCKET(Service), {<<"default">>,
                                    ?PCD_ARRAYS_PATH(atom_to_binary(Service, utf8))}).
-define(PCD_ARRAYS_CHUNKS_PATH(Service), << (Service)/binary,
                                          "-",
                                          "array_chunks" >>).
-define(PCD_ARRAYS_CHUNKS_BUCKET(Service), {<<"default">>,
                                          ?PCD_ARRAYS_CHUNKS_PATH(atom_to_binary(Service, utf8))}).

-define(PCD_ARRAY_KEY(ID, NR), #chunk_key{id = ID, chunk_nr = NR}).
-define(PCD_ARRAY_DB(Array), (Array#pcd_array.db_module)).

-export_type([pcd_array/0]).

-record(pcd_row,
        {
            dirty               = false                 :: boolean(),
            first_empty_slot    = -1                    :: integer(),
            nr_of_empty_slots   = ?PCD_DEFAULT_ROW_SIZE :: non_neg_integer(),
            data                                        :: array:array(),
            delayed_pids        = []                    :: list(pid())
        }).

-record(pcd_array,
        {
            row_size        = ?PCD_DEFAULT_ROW_SIZE     :: pos_integer(),
            nr_of_rows      = 0                         :: non_neg_integer(),
            rows                                        :: array:array(pcd_row()),
            rows_with_empty_slots = []                  :: list(non_neg_integer()),
            persistent      = true                      :: boolean(),
            id              = <<"">>                    :: binary(),
            owner_of_db     = undefined                 :: atom(),
            db_module       = ?PCD_DEFAULT_DB_MODULE    :: atom(),
            relief_fun      = undefined                 :: fun()
        }).

-opaque pcd_array() :: #pcd_array{}.
-opaque pcd_row() :: #pcd_row{}.

-record(chunk_key,
        {
            id              = <<"">>                    :: binary(),
            chunk_nr                                    :: pos_integer()
        }).

