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
-include("../src/pcd.hrl").

-define(NR_OF_ELEMS, 3).
-define(TEST_ELEM, <<"Test Elem">>).

suite() -> [].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_, Config) ->
    Config.

all() -> [basic_mem, basic_db, delayed_write].

basic_mem(_Config) ->
    A = pcd_array:load(pcd, <<"TESTCACHE">>, false, 2, ?PCD_DEFAULT_DB_MODULE),
    A1 = populate(A, ?NR_OF_ELEMS - 1),
    true = pcd_array:check_health(A1),
    {Ix0, A2} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A1),
    {ok, {?NR_OF_ELEMS, ?TEST_ELEM}, _} = pcd_array:get_elem(Ix0, A2),
    true = pcd_array:check_health(A2).


basic_db(_Config) ->
    A = pcd_array:load(pcd, <<"TESTCACHE">>, true, 2, ?PCD_DEFAULT_DB_MODULE),
    A1 = populate(A, ?NR_OF_ELEMS - 1),
    true = pcd_array:check_health(A1),
    A0 = pcd_array:load(pcd, <<"TESTCACHE">>, true, 2, ?PCD_DEFAULT_DB_MODULE),
    {Ix0, A2} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A0),
    {ok, {?NR_OF_ELEMS, ?TEST_ELEM}, _} = pcd_array:get_elem(Ix0, A2),
    true = pcd_array:check_health(A2),
    A3 = pcd_array:delete_elem(Ix0, A2),
    true = pcd_array:check_health(A3),
    pcd_array:delete(A3).

delayed_write(_Config) ->
    A0 = pcd_array:load(pcd, <<"TESTCACHE">>, true, 2, ?PCD_DEFAULT_DB_MODULE),
    A = pcd_array:set_delayed_write_fun(A0, fun delayed_fun/2),
    {_, A1} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A, self()),
    {_, A2} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A1, self()),
    {_, A3} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A2, self()),
    {_, A4} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A3, self()),
    {_, A5} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A4, self()),
    {_, A6} = pcd_array:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A5, self()),
    pcd_array:write(A6),
    pcd_array:delete(A6).
    
delayed_fun(_Ix, _Args) ->
    ok.
%    ct:print("ARGS:~p", [{Ix, Args}]).

populate(A, 0) ->
    A;
populate(A, N) ->
    {_X, A1} = pcd_array:add_elem({N, "content"}, A),
    populate(A1, N - 1).    

