-define(PSH_DEFAULT_ARRAY_ROW_SIZE, 100).
-define(PSH_ARRAYS_PATH(Service), << (Service)/binary,
                                   "-",
                                   "arrays" >>).
-define(PSH_ARRAYS_BUCKET(Service), {<<"default">>,
                                    ?PSH_LISTS_PATH(atom_to_binary(Service, utf8))}).
-define(PSH_ARRAYS_CHUNKS_PATH(Service), << (Service)/binary,
                                          "-",
                                          "array_chunks" >>).
-define(PSH_ARRAYS_CHUNKS_BUCKET(Service), {<<"default">>,
                                          ?PSH_LISTS_CHUNKS_PATH(atom_to_binary(Service, utf8))}).

-define(PSH_ARRAY_KEY(ID, NR), #chunk_key{id = ID, chunk_nr = NR}).

-export_type([pcd_list/0]).

-record(chunk,
        {
         next_empty         = 0                         :: non_neg_integer(),
         elems                                          :: array:array()
        }).

-type chunk() :: #chunk{}.

-record(pcd_list,
        {
            cache_size      = ?PSH_DEFAULT_LIST_SIZE    :: pos_integer(),
            nr_of_chunks    = 0                         :: non_neg_integer(),
            cached_data                                 :: chunk(),
            persistent      = true                      :: boolean(),
            id              = <<"">>                    :: binary(),
            interim_data                                :: chunk(),
            interim_chunk_nr= none                      :: none | non_neg_integer(),
            owner_of_db     = undefined                 :: atom()
        }).

-opaque pcd_list() :: #pcd_list{}.

-record(chunk_key,
        {
            id              = <<"">>                    :: binary(),
            chunk_nr                                    :: pos_integer()
        }).

