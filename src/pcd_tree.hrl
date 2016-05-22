-include("pcd_common.hrl").
-define(PCD_DEFAULT_TREE_SIZE, 500).
-define(PCD_TREE_PATH(Service), << (Service)/binary,
                                   "-",
                                   "trees" >>).
-define(PCD_TREES_BUCKET(Service), {<<"default">>,
                                    ?PCD_TREES_PATH(atom_to_binary(Service, utf8))}).
-define(PCD_TREES_CHUNKS_PATH(Service), << (Service)/binary,
                                          "-",
                                          "tree_chunks" >>).
-define(PCD_TREES_CHUNKS_BUCKET(Service), {<<"default">>,
                                          ?PCD_TREES_CHUNKS_PATH(atom_to_binary(Service, utf8))}).

-define(PCD_TREE_KEY(ID, NR), #chunk_key{id = ID, chunk_nr = NR}).
-define(PCD_TREE_DB(Array), (Array#pcd_tree.db_module)).

-export_type([pcd_tree/0, tree_index/0]).

-type tree_index() :: non_neg_integer().

-record(pcd_tree_row,
        {
            dirty               = false                 :: boolean(),
            nr_of_empty_slots   = ?PCD_DEFAULT_ROW_SIZE :: non_neg_integer(),
            data                                        :: pcd_tree:pcd_tree()
        }).

-record(pcd_tree,
        {
            row_size        = ?PCD_DEFAULT_ROW_SIZE     :: pos_integer(),
            nr_of_rows      = 0                         :: non_neg_integer(),
            rows                                        :: tree:tree(pcd_row()),
            rows_with_empty_slots = []                  :: list(non_neg_integer()),
            persistent      = true                      :: boolean(),
            id              = <<"">>                    :: binary(),
            owner_of_db     = undefined                 :: atom(),
            db_module                                   :: atom(),
            relief_fun      = undefined                 :: undefined | fun(),
            nr_of_elems     = 0                         :: non_neg_integer(),
            delayed_row_nrs = []                        :: list(non_neg_integer()),
            delayed_row_params                          :: tree:tree(row_param())
        }).

-opaque pcd_tree() :: #pcd_tree{}.


