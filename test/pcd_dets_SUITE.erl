%%%-------------------------------------------------------------------
%%% @author zsoci
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Dec 2016 9:05 PM
%%%-------------------------------------------------------------------
-module(pcd_dets_SUITE).

-export([suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         all/0]).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
%-include_lib("eunit/include/eunit.hrl").
%-include_lib("kernel/include/file.hrl").

-define(NR_OF_ELEMS, 10).
-define(TEST_ELEM, <<"Test Elem">>).
-define(LOAD_CHUNK_SIZE, 4).
-define(NR_OF_LOAD_ELEMS, 10).

-define(BUCKET, {<<"default">>, <<"bucket">>}).
-define(KEY, <<"KEY">>).
-define(OWNER, owner).
-define(VALUE, <<"Value">>).
-define(NEWVALUE, "New Value").

%% -define(LOAD_CHUNK_SIZE, 500).
%% -define(NR_OF_LOAD_ELEMS, 1000000).

suite() -> [].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_, Config) ->
  Config.

all() -> [store_retrieve
%%           large_mem_array,
%%           large_mem_list
].

store_retrieve(_Config) ->
  ok = pcd_db_dets:store(?BUCKET,?KEY,?VALUE,?MODULE, ?OWNER),
  {ok, ?VALUE} = pcd_db_dets:fetch(?BUCKET, ?KEY, ?MODULE, ?OWNER),
  {ok, ?VALUE, Object} = pcd_db_dets:get(?BUCKET, ?KEY, ?MODULE, ?OWNER),
  ok = pcd_db_dets:update(Object, ?NEWVALUE, ?MODULE, ?OWNER),
  {ok, ?NEWVALUE} = pcd_db_dets:fetch(?BUCKET, ?KEY, ?MODULE, ?OWNER),
  ok = pcd_db_dets:delete(?BUCKET, ?KEY, ?MODULE, ?OWNER),
  {error, notfound} = pcd_db_dets:fetch(?BUCKET, ?KEY, ?MODULE, ?OWNER).
