-include("pcd_list.hrl").
-include("pcd_array.hrl").
-include("pcd_tree.hrl").

-define(PCD_DEFAULT_DB_MODULE, pcd_db_riak).
-export_type([dtype/0, pcd_index/0]).

-type dtype() :: pcd_list() | pcd_array() | pcd_tree().
-type pcd_index() :: list_index() | array_index() | tree_index().