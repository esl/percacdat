%% @author zsoci
%% @doc @todo Add description to pcd_db_hanlder.


-module(pcd_db_hanlder).

%% ====================================================================
%% API functions
%% ====================================================================
-include_lib("mixer/include/mixer.hrl").
-mixin([{ pcd_db_riak,
          [init/1, terminate/1,
           fetch/4, fetch/5, fetch_keep/4,
           store/5, store/6, store_keep/5,
           get/4, update/5, update/4, update_keep/4,
           fetch_type/4, update_type/5, update_type/6,
           close/0,
           delete/4, delete_keep/4,
           delete_object/2, delete_object/3, delete_object_keep/2]}]).

%% ====================================================================
%% Internal functions
%% ====================================================================


