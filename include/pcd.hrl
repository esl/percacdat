-export_type([pcd_dtype/0, pcd_index/0]).

-opaque pcd_dtype() :: {pcd_array, pcd_array:data()}
                     | {pcd_list, pcd_list:data()}
                     | {pcd_tree, pcd_tree:data()}.

-opaque pcd_index() :: pcd_list:index()
                     | pcd_array:index()
                     | pcd_tree:index().

-type dtype() :: pcd_list:data()
               | pcd_array:data()
               | pcd_tree:data().