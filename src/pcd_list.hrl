-define(PCD_DEFAULT_LIST_SIZE, 100).
-define(PCD_LISTS_PATH(Service), << (Service)/binary,
                                   "-",
                                   "lists" >>).
-define(PCD_LISTS_BUCKET(Service), {<<"default">>,
                                    ?PCD_LISTS_PATH(atom_to_binary(Service, utf8))}).
-define(PCD_LISTS_CHUNKS_PATH(Service), << (Service)/binary,
                                          "-",
                                          "list_chunks" >>).
-define(PCD_LISTS_CHUNKS_BUCKET(Service), {<<"default">>,
                                          ?PCD_LISTS_CHUNKS_PATH(atom_to_binary(Service, utf8))}).

-define(PCD_CHUNK_KEY(ID, NR), #chunk_key{id = ID, chunk_nr = NR}).
-define(PCD_LIST_DB(Cache), (Cache#pcd_list.db_module)).

-export_type([data/0, index/0]).

-opaque index() :: non_neg_integer().

-record(chunk,
        {
         next_empty         = 0                         :: non_neg_integer(),
         elems                                          :: array:array()
        }).

-type chunk() :: #chunk{}.

-record(pcd_list,
        {
            cache_size      = ?PCD_DEFAULT_LIST_SIZE    :: pos_integer(),
            nr_of_chunks    = 0                         :: non_neg_integer(),
            cached_data                                 :: chunk(),
            persistent      = true                      :: boolean(),
            id              = <<"">>                    :: binary(),
            interim_data                                :: chunk(),
            interim_chunk_nr= none                      :: none | non_neg_integer(),
            owner_of_db     = undefined                 :: atom(),
            db_module                                   :: atom(),
            relief_fun      = undefined                 :: undefined | fun()
        }).

-opaque data() :: #pcd_list{}.


