%% @author zsoci
%% @doc @todo Add description to pcd_test.


-module(pcd_test).

%% ====================================================================
%% API functions
%% ====================================================================
-export([t_new/0,
         t_add/1,
         t_add/0,
         t_get/1,
         populate/2
        ]).

t_new() ->
    pcd_array:load(pcd, <<"TESTCACHE">>).

%% t_delete(C) ->
%%     delete(C).
%%
t_add() ->
    t_add(1).

t_add(N) ->
    A1 = pcd_array:load(pcd, <<"TESTCACHE">>, true, 4),
    {Time, Value} =timer:tc(?MODULE, populate, [A1, N]),
    io:format("taken:~p~nSize:~p", [Time/1000000, erts_debug:flat_size(Value)]),
    Value.

t_get(N) ->
    A1 = pcd_array:load(pcd, <<"TESTCACHE">>),
    pcd_array:get_elem(N, A1).
%% ====================================================================
%% Internal functions
%% ====================================================================

populate(Cache, 0) ->
    Cache;
populate(Cache, N) ->
    _V = {_Ix, C1} = pcd_array:add_elem({N, "maeslkdjf;l ajsdf; j;asdfj ;slajdf l;jsad; ljelement"}, Cache),
    populate(C1, N - 1).


