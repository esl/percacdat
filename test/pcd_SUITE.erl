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
-define(LOAD_CHUNK_SIZE, 4).
-define(NR_OF_LOAD_ELEMS, 10).
%% -define(LOAD_CHUNK_SIZE, 500).
%% -define(NR_OF_LOAD_ELEMS, 1000000).

suite() -> [].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_, Config) ->
    Config.

all() -> [basic_mem
%%           large_mem_array,
%%           large_mem_list
         ].

basic_mem(_Config) ->
    A = pcd:load(pcd_array, pcd, <<"TESTCACHE">>, false, 4),
    A1 = populate(A, ?NR_OF_ELEMS - 1),
    true = pcd:check_health(A1),
    {Ix0, A2} = pcd:add_elem({?NR_OF_ELEMS, ?TEST_ELEM}, A1),
    {ok, {?NR_OF_ELEMS, ?TEST_ELEM}, _} = pcd:get_elem(Ix0, A2),
    true = pcd:check_health(A2).

large_mem_array(_Config) ->
    A1 = pcd_array:load(test, <<"TESTCACHE">>, false, ?LOAD_CHUNK_SIZE),
    {Time, Value} =timer:tc(?MODULE, populate_array, [A1, ?NR_OF_LOAD_ELEMS]),
    {Time1, Value1} = timer:tc(?MODULE, delete_array, [Value, ?NR_OF_LOAD_ELEMS]),
    ct:print("array taken:~p~nSize:~p", [Time/1000000, erts_debug:flat_size(Value)]),
    ct:print("array delete taken:~p~nSize:~p", [Time1/1000000, erts_debug:flat_size(Value1)]),
    A2 = pcd:load(pcd_array, test, <<"TESTCACHE">>, false, ?LOAD_CHUNK_SIZE),
    {Time2, Value2} =timer:tc(?MODULE, populate, [A2, ?NR_OF_LOAD_ELEMS]),
    ct:print("pcd array taken:~p~nSize:~p", [Time2/1000000, erts_debug:flat_size(Value2)]),
    {Time3, Value3} = timer:tc(?MODULE, delete, [Value2, ?NR_OF_LOAD_ELEMS]),
    ct:print("pcd array delete taken:~p~nSize:~p", [Time3/1000000, erts_debug:flat_size(Value3)]).

large_mem_list(_Config) ->
    A1 = pcd_list:load(test, <<"TESTCACHE">>, false, ?LOAD_CHUNK_SIZE),
    {Time, Value} =timer:tc(?MODULE, populate_list, [A1, ?NR_OF_LOAD_ELEMS]),
    ct:print("list taken:~p~nSize:~p", [Time/1000000, erts_debug:flat_size(Value)]),
    A2 = pcd:load(pcd_list, test, <<"TESTCACHE">>, false, ?LOAD_CHUNK_SIZE),
    {Time2, Value2} =timer:tc(?MODULE, populate, [A2, ?NR_OF_LOAD_ELEMS]),
    ct:print("pcd list taken:~p~nSize:~p", [Time2/1000000, erts_debug:flat_size(Value2)]).

populate(A, 0) ->
    A;
populate(A, N) ->
    {X, A1} = pcd:add_elem({N, "content"}, A),
    populate(A1, N - 1).

delete(A, 0) ->
    A;
delete(A, N) ->
    A1 = pcd:delete_elem(N - 1, A),
    delete(A1, N - 1).

populate_array(A, 0) ->
    A;
populate_array(A, N) ->
    {X, A1} = pcd_array:add_elem({N, "content"}, A),
    populate_array(A1, N - 1).    

populate_list(A, 0) ->
    A;
populate_list(A, N) ->
    {X, A1} = pcd_list:add_elem({N, "content"}, A),
    populate_list(A1, N - 1).

delete_array(A, 0) ->
    A;
delete_array(A, N) ->
    A1 = pcd_array:delete_elem(N - 1, A),
    delete_array(A1, N - 1).

