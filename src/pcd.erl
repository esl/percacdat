%% @author zsoci
%% @doc @todo Add description to pcd.


-module(pcd).

-include("pcd_common.hrl").
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
         update_elem_in_cache/3,
         check_health/1,
         last_index/1,
         first_index/1,
         next_index/2,
         prev_index/2,
         to_list/1,
         to_list/3
        ]).

-callback load(Owner :: atom(),
               Id :: binary(),
               Persistent :: boolean(),
               ChunkSize :: pos_integer(),
               DBModule :: atom()) ->
    Result :: {ok, Data :: dtype()}
            | {error, Reason :: term()}.
-callback set_delayed_write_fun(Data :: dtype(),
                                Fun :: fun()) ->
    Result :: dtype().
-callback add_elem(Element :: term(), Data :: dtype()) ->
    Result :: {ok, Index :: pcd_index(), NewData :: dtype()}
            | {error, Reason :: term()}.
-callback add_elem(Element :: term(), Data :: dtype(), Params :: term()) ->
    Result :: {ok, Index :: pcd_index(), NewData :: dtype()}
            | {error, Reason :: term()}.
-callback get_elem(Index :: pcd_index(), Data :: dtype()) ->
    Result :: {ok, Value :: term(), NewData :: dtype()}
            | {undefined, NewData ::dtype()}
            | {error, Reason :: term()}.
-callback delete_elem(Index :: pcd_index(), Data :: dtype()) ->
    Result :: {ok, NewData :: dtype()}
            | {undefined, NewData :: dtype()}
            | {error, Reason :: term(), NewData :: dtype()}.
-callback delete_elem(Index :: pcd_index(), Data :: dtype(), Params :: term()) ->
    Result :: {ok, NewData :: dtype()}
            | {undefined, NewData :: dtype()}
            | {error, Reason :: term()}.
-callback delete(Data :: dtype()) ->
    Result :: ok
            | {error, Reason :: term()}.
-callback write(Data :: dtype()) ->
    Result :: term().
-callback update_elem(Index :: pcd_index(), Elem :: term(), Data :: dtype()) ->
    Result :: {ok, dtype()}
            | {undefined, NewData :: dtype()}
            | {error, Reason :: term()}.
-callback update_elem(Index :: pcd_index(), Elem :: term(), Data :: dtype(), Params :: term()) ->
    Result :: {ok, NewData :: dtype()}
            | {undefined, NewData :: dtype()}
            | {error, Reason :: term()}.
-callback update_elem_in_cache(Index :: pcd_index(), Elem :: term(), Data :: dtype()) ->
    Result :: {ok, dtype()}
            | {undefined, NewData :: dtype()}
            | {error, Reason :: term()}.
-callback last_index(Data :: dtype()) ->
    Result :: term()
            | {error, Reason :: term()}.
-callback first_index(Data :: dtype()) ->
    Result :: term()
            | {error, Reason :: term()}.
-callback next_index(Data :: dtype(), Index :: term()) ->
    Result :: {ok, NewIndex :: term(), NewData :: dtype()}
            | {error, Reason :: term()}.
-callback prev_index(Data :: dtype(), Index :: term()) ->
    Result :: {ok, NewIndex :: term(), NewData :: dtype()}
            | {error, Reason :: term()}.

load(Type, Owner, Id, Persistent, Size, DBModule) ->
    case Type:load(Owner, Id, Persistent, Size, DBModule) of
        {error, Reason} ->
            {error, Reason};
        {ok, Data} ->
            {Type, Data}
    end.
load(Type, Owner, Id, Persistent, Size) ->
    load(Type, Owner, Id, Persistent, Size, ?PCD_DEFAULT_DB_MODULE).
load(Type, Owner, Id, Persistent) ->
    load(Type, Owner, Id, Persistent, ?PCD_DEFAULT_CHUNK_SIZE, ?PCD_DEFAULT_DB_MODULE).
load(Type, Owner, Id) ->
    load(Type, Owner, Id, true, ?PCD_DEFAULT_CHUNK_SIZE, ?PCD_DEFAULT_DB_MODULE).
load(Type) ->
    load(Type, undefined, <<"undefined">>, false, ?PCD_DEFAULT_CHUNK_SIZE, ?PCD_DEFAULT_DB_MODULE).

set_delayed_write_fun({Type, Data}, Fun) ->
    {Type, Type:set_delayed_write_fun(Data, Fun)}.

-spec add_elem(any(), pcd_dtype()) ->
          {ok, pcd_index(), pcd_dtype()}
        | {error, any()}.
add_elem(Elem, {Type, Data}) ->
    case Type:add_elem(Elem, Data) of
        {error, Reason} ->
            {error, Reason};
        {ok, Ix, RetVal} ->
            {ok, Ix, {Type, RetVal}}
    end.

-spec add_elem(any(), pcd_dtype(), any()) ->
          {ok, pcd_index(), pcd_dtype()}
        | {error, any()}.
add_elem(Elem, {Type, Data}, Params) ->
    case Type:add_elem(Elem, Data, Params) of
        {error, Reason} ->
            {error, Reason};
        {ok, Ix, RetVal} ->
            {ok, Ix, {Type, RetVal}}
    end.

get_elem(Index, {Type, Data}) ->
    case Type:get_elem(Index, Data) of
        {ok, Value, NewData} ->
            {ok, Value, {Type, NewData}};
        {undefined, NewData} ->
            {undefined, {Type, NewData}};
        Else ->
            Else
    end.

delete({Type, Data}) ->
    Type:delete(Data).

delete_elem(Index, {Type, Data}) ->
    case Type:delete_elem(Index, Data) of
        {error, Reason} ->
            {error, Reason};
        {Result, NewData} ->
            {Result, {Type, NewData}}
    end.

delete_elem(Index, {Type, Data}, Params) ->
    case Type:delete_elem(Index, Data, Params) of
        {error, Reason} ->
            {error, Reason};
        {Result, NewData} ->
            {Result, {Type, NewData}}
    end.

update_elem(Index, Elem, {Type, Data}) ->
    case Type:update_elem(Index, Elem, Data) of
        {Result, NewData} ->
            {Result, {Type, NewData}};
        Else ->
            Else
    end.

update_elem(Index, Elem, {Type, Data}, Params) ->
    case Type:update_elem(Index, Elem, Data, Params) of
        {Result, NewData} ->
            {Result, {Type, NewData}};
        Else ->
            Else
    end.

update_elem_in_cache(Index, Elem, {Type, Data}) ->
    case Type:update_elem_in_cache(Index, Elem, Data) of
        {Result, NewData} ->
            {Result, {Type, NewData}};
        Else ->
            Else
    end.

write({Type, Data}) ->
    Type:write(Data).

check_health({Type, Data}) ->
    Type:check_health(Data).

last_index({Type, Data}) ->
    Type:last_index(Data).
first_index({Type, Data}) ->
    Type:first_index(Data).
next_index({Type, Data}, Index) ->
    case Type:next_index(Data, Index) of
        {ok, NewIndex, NewData} ->
            {ok, NewIndex, {Type, NewData}};
        Else ->
            Else
    end.

prev_index({Type, Data}, Index) ->
    case Type:prev_index(Data, Index) of
        {ok, NewIndex, NewData} ->
            {ok, NewIndex, {Type, NewData}};
        Else ->
            Else
    end.

-spec to_list(Data :: pcd_dtype()) -> list().
to_list(Data) ->
    First = pcd:first_index(Data),
    to_list(Data, First, pcd:last_index(Data) - First + 1).

to_list(Data, Start, NrOfElems) ->
    get_elems(Data, Start, NrOfElems, []).

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec get_elems(pcd_dtype(), pcd_index(), non_neg_integer(), list()) -> list().
get_elems(_, _, 0, List) ->
    List;
get_elems(Data, Start, NrOfElems, List) ->
    case get_elem(Start, Data) of
        {ok, Elem, NewData} ->
            case next_index(NewData, Start) of
                {ok, NextIndex, NewData2} ->
                    get_elems(NewData2, NextIndex, NrOfElems - 1, [Elem | List]);
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

