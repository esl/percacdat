%% @author zsoci
%% @doc @todo Add description to pcd.


-module(pcd).

-include("pcd.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([load/1,
         load/3,
         load/4,
         load/5,
         load/6,
         set_delayed_write_fun/2,
         get_elem/2,
         add_elem/2,
         add_elem/3,
         delete_elem/2,
         delete_elem/3,
         delete/1,
         write/1,
         update_elem/3,
         update_elem/4,
         check_health/1,
         last_index/1
        ]).

-callback load(Owner :: atom(),
               Id :: binary(),
               Persistent :: boolean(),
               ChunkSize :: pos_integer(),
               DBModule :: atom()) ->
    Result :: dtype().
-callback set_delayed_write_fun(Data :: dtype(),
                                Fun :: fun()) ->
    Result :: dtype().
-callback add_elem(Element :: term(), Data :: dtype()) ->
    Result :: {Index :: pcd_index(), NewData :: dtype()}
            | {error, Reason :: term()}.
-callback add_elem(Element :: term(), Data :: dtype(), Params :: term()) ->
    Result :: {Index :: pcd_index(), NewData :: dtype()}
            | {error, Reason :: term()}.
-callback get_elem(Index :: pcd_index(), Data :: dtype()) ->
    Result :: {ok, Value :: term()}
            | undefined.
-callback delete_elem(Index :: pcd_index(), Data :: dtype()) ->
    Result :: dtype()
            | undefined.
-callback delete_elem(Index :: pcd_index(), Data :: dtype(), Params :: term()) ->
    Result :: dtype()
            | undefined.
-callback delete(Data :: dtype()) ->
    Result :: ok
            | {error, Reason :: term()}.
-callback write(Data :: dtype()) ->
    Result :: term().
-callback update_elem(Index :: pcd_index(), Elem :: term(), Data :: dtype()) ->
    Result :: dtype()
            | undefined.
-callback update_elem(Index :: pcd_index(), Elem :: term(), Data :: dtype(), Params :: term()) ->
    Result :: dtype()
            | undefined.
-callback last_index(Data :: dtype()) ->
    Result :: term().

load(Type, Owner, Id, Persistent, Size, DBModule) ->
    {Type, Type:load(Owner, Id, Persistent, Size, DBModule)}.
load(Type, Owner, Id, Persistent, Size) ->
    load(Type, Owner, Id, Persistent, Size, ?PCD_DEFAULT_DB_MODULE).
load(Type, Owner, Id, Persistent) ->
    load(Type, Owner, Id, Persistent, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).
load(Type, Owner, Id) ->
    load(Type, Owner, Id, true, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).
load(Type) ->
    load(Type, undefined, <<"undefined">>, false, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).

set_delayed_write_fun({Type, Data}, Fun) ->
    {Type, Type:set_delayed_write_fun(Data, Fun)}.

add_elem(Elem, {Type, Data}) ->
    {Ix, RetVal} = Type:add_elem(Elem, Data),
    {Ix, {Type, RetVal}}.

add_elem(Elem, {Type, Data}, Params) ->
    {Ix, RetVal} = Type:add_elem(Elem, Data, Params),
    {Ix, {Type, RetVal}}.

get_elem(Index, {Type, Data}) ->
    Type:get_elem(Index, Data).

delete({Type, Data}) ->
    Type:delete(Data).

delete_elem(Index, {Type, Data}) ->
    {Type, Type:delete_elem(Index, Data)}.

delete_elem(Index, {Type, Data}, Params) ->
    {Type, Type:delete_elem(Index, Data, Params)}.

update_elem(Index, Elem, {Type, Data}) ->
    {Type, Type:update_elem(Index, Elem, Data)}.

update_elem(Index, Elem, {Type, Data}, Params) ->
    {Type, Type:update_elem(Index, Elem, Data, Params)}.

write({Type, Data}) ->
    Type:write(Data).

check_health({Type, Data}) ->
    Type:check_health(Data).

last_index({Type, Data}) ->
    Type:last_index(Data).
%% ====================================================================
%% Internal functions
%% ====================================================================


