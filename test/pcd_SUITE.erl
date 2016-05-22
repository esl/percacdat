-module(pcd_SUITE).

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

all() -> [basic_mem,
          large_mem].

basic_mem(_Config) ->
    A = pcd:load(pcd_array, pcd, <<"TESTCACHE">>, false, 4),
    A1 = populate(A, ?NR_OF_ELEMS - 1),
    true = pcd:check_health(A1),
    {Ix0, A2} = pcd:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A1),
    {ok, {?NR_OF_ELEMS, ?TEST_ELEM}} = pcd:get_elem(Ix0, A2),
    true = pcd:check_health(A2).


large_mem(_Config) ->
    A1 = pcd_array:load(test, <<"TESTCACHE">>, false, 500),
    {Time, Value} =timer:tc(?MODULE, populate_array, [A1, 1000000]),
    ct:print("taken:~p~nSize:~p", [Time/1000000, erts_debug:flat_size(Value)]),
    A2 = pcd:load(pcd_array, test, <<"TESTCACHE">>, false, 500),
    {Time2, Value2} =timer:tc(?MODULE, populate, [A2, 1000000]),
    ct:print("taken:~p~nSize:~p", [Time2/1000000, erts_debug:flat_size(Value2)]).

populate(A, -1) ->
    A;
populate(A, N) ->
    {X, A1} = pcd:add_elem({N, "content"}, A),
    populate(A1, N - 1).

populate_array(A, -1) ->
    A;
populate_array(A, N) ->
    {X, A1} = pcd_array:add_elem({N, "content"}, A),
    populate_array(A1, N - 1).    

