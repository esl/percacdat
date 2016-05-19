-module(pcd_array_SUITE).

-export([suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         all/0]).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
%-include_lib("kernel/include/file.hrl").

-define(NR_OF_ELEMS, 10).
-define(TEST_ELEM, <<"Test Elem">>).

suite() -> [].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_, Config) ->
    Config.

all() -> [basic_mem, basic_db].

basic_mem(Config) ->
    A = pcd_array:load(pcd, <<"TESTCACHE">>, false, 4),
    A1 = populate(A, ?NR_OF_ELEMS - 1),
    true = pcd_array:check_health(A1),
    {Ix0, A2} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A1),
    {ok, {?NR_OF_ELEMS, ?TEST_ELEM}} = pcd_array:get_elem(Ix0, A2).
    true = pcd_array:check_health(A2),


basic_db(_Config) ->
    ok.

populate(A, -1) ->
    A;
populate(A, N) ->
    ct:print("To add(~p):~p", [N, A]),
    {X, A1} = pcd_array:add_elem({N, "content"}, A),
    ct:print("Added(~p):~p", [X, A1]),
    populate(A1, N - 1).    

