%% @author zsoci
%% @doc @todo Add description to PCD_pcarray.


-module(pcd_array).
-include("pcd_array.hrl").

-json_opt({type_field, [pcd_array, chunk_key]}).

-json({pcd_array,
       {number, "row_size", [{default, ?PCD_DEFAULT_ROW_SIZE}]},
       {skip, [{default, 0}]},  % nr of rows is calculated by collecting the chunks
       skip,  % rows as an array
       {skip, [{default, []}]},  % rows_with_empty_slots
       {boolean, "persistent"},
       {binary, "id"},
       {atom, "owner_of_db"},
       {atom, "db_module", [{default, pcd_db_riak}]},
       skip,
       {skip, [{default, 0}]}}). % nr_of_elems

-json({chunk_key,
       {binary, "id"},
       {number, "chunk_nr"}}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([load/3,
         load/2,
         load/4,
         load/0,
         set_delayed_write_fun/2,
         get_elem/2,
         add_elem/2,
         delete_elem/2,
         delete/1,
         check_health/1
        ]).

-export([to_json/1,
         from_json/1]).

-export([t_new/0,
         get_local_index/2,
%         t_delete/1,
         populate/2,
         t_add/1,
         t_add/0,
         t_get/1
        ]).

%% load(Owner, ID)
%% load(Owner, ID, Persistent)
%% load(Owner, ID, Persistent, Size)
%% @doc
%% Load or create a new persistent array data structure
%% Owner :: atom to retrieve riak_db connection from application environment
%% Id :: Unique Id used as the key of the set in RIAK
%% Persistent :: true if we want to have the pcd_list persistent
%% Size :: size of one row
%% @end
-spec load(Owner, Id, Persistent, Size, DBModule) -> Result when
          Owner :: atom(),
          Id :: binary(),
          Persistent :: boolean(),
          Size :: pos_integer(),
          DBModule :: atom(),
          Result :: pcd_array()
                  | {error, term()}.
load(Owner, Id, Persistent, Size, DBModule) ->
    case Persistent of
        true ->
            maybe_load_from_db(Owner, Id, Size, DBModule);
        false ->
            create_new_array(Owner, Id, false, Size, DBModule)
    end.
load(Owner, Id, Persistent, Size) ->
    load(Owner, Id, Persistent, Size, ?PCD_DEFAULT_DB_MODULE).
load(Owner, Id, Persistent) ->
    load(Owner, Id, Persistent, ?PCD_DEFAULT_ROW_SIZE, ?PCD_DEFAULT_DB_MODULE).
load(Owner, Id) ->
    load(Owner, Id, true, ?PCD_DEFAULT_ROW_SIZE, ?PCD_DEFAULT_DB_MODULE).
load() ->
    load(undefined, <<"undefined">>, false, ?PCD_DEFAULT_ROW_SIZE, ?PCD_DEFAULT_DB_MODULE).

-spec set_delayed_write_fun(Array :: pcd_array(),
                            Fun :: fun()) ->
                                Reply :: pcd_array().
set_delayed_write_fun(Array, Fun) ->
    Array#pcd_array{relief_fun = Fun}.

-spec get_local_index(GlobalIndex :: non_neg_integer(),
                      Array :: pcd_array()) -> {Row :: non_neg_integer(),
                                                Column :: non_neg_integer()}.
get_local_index(GlobalIndex, Array) ->
    local_index(GlobalIndex, Array#pcd_array.row_size).

delete(Array) ->
    case Array#pcd_array.persistent of
        true ->
            case ?PCD_ARRAY_DB(Array):delete(?PCD_ARRAYS_BUCKET(Array#pcd_array.owner_of_db),
                                             Array#pcd_array.id,
                                             ?MODULE,
                                             Array#pcd_array.owner_of_db) of
                ok ->
                    delete_chunks(Array);
                Else ->
                    lager:error("~p. Could not delete cache ~p", [Else, Array]),
                    Else
            end;
        false ->
            ok
    end.

-spec add_elem(Element :: term(), Array :: pcd_array()) -> Result when
          Result :: {non_neg_integer(), pcd_array()}
                  | {error, term()}.
add_elem(Element, Array) ->
    % find a row with empty slots first
    case Array#pcd_array.rows_with_empty_slots of
        [] ->
            % no empty slot, add a row and retry
            add_elem(Element, create_new_row(Array));
        [RowX | MayKeepList] ->
            add_elem_to_row(RowX, Element, Array, MayKeepList)
    end.

add_elem_to_row(RowX, Element, Array, MayKeepList) ->
    Row = array:get(RowX, Array#pcd_array.rows),
    EmptyX = Row#pcd_row.first_empty_slot,
    case array:get(EmptyX, Row#pcd_row.data) of
        {empty, NextEmptyX} ->
            NewNrOfEmptySlots = Row#pcd_row.nr_of_empty_slots - 1,
            NewRow = Row#pcd_row{nr_of_empty_slots = NewNrOfEmptySlots,
                                 dirty = true,
                                 data = array:set(EmptyX, {elem, Element}, Row#pcd_row.data),
                                 first_empty_slot = NextEmptyX},
            GlobalIndex = global_index(RowX, EmptyX, Array#pcd_array.row_size),
            NrOfElems = Array#pcd_array.nr_of_elems + 1,
            SavedArray = update_chunk_db(Array#pcd_array{nr_of_elems = NrOfElems}, RowX, NewRow),
            case NewNrOfEmptySlots of
                0 ->
                    {GlobalIndex, SavedArray#pcd_array{rows_with_empty_slots = MayKeepList}};
                _ ->
                    {GlobalIndex, SavedArray}
            end;
        _Else ->
            {error, notempty}
    end.

-spec delete_elem(GlobalIndex :: non_neg_integer(), Array :: pcd_array()) -> Reply
    when Reply :: pcd_array()
                | undefined.
delete_elem(GlobalIndex, Array) ->
    case get_elem(GlobalIndex, Array) of
        {ok, _} ->
            {RowX, ColumnX} = local_index(GlobalIndex, Array#pcd_array.row_size),
            Row = array:get(RowX, Array#pcd_array.rows),
            FirstEmpty = Row#pcd_row.first_empty_slot,
            NrOfEmptySlots = Row#pcd_row.nr_of_empty_slots + 1,
            NewRow = Row#pcd_row{data = array:set(ColumnX, {empty, FirstEmpty}, Row#pcd_row.data),
                                 dirty = true,
                                 first_empty_slot = ColumnX,
                                 nr_of_empty_slots = NrOfEmptySlots},
            RowsWithEmptySlots = case NrOfEmptySlots of
                                     1 ->
                                         [RowX | Array#pcd_array.rows_with_empty_slots];
                                     _ ->
                                         Array#pcd_array.rows_with_empty_slots
                                 end,
            NrOfElems = Array#pcd_array.nr_of_elems - 1,
            update_chunk_db(Array#pcd_array{rows_with_empty_slots = RowsWithEmptySlots,
                                            nr_of_elems = NrOfElems},
                            RowX,
                            NewRow);
        _ ->
            undefined
    end.

-spec get_elem(GlobalIndex :: non_neg_integer(), Array :: pcd_array()) -> Reply
    when Reply :: {ok, Value :: term()}
                | undefined.
get_elem(GlobalIndex, Array) ->
    {RowX, ColumnX} = local_index(GlobalIndex, Array#pcd_array.row_size),
    Row = array:get(RowX, Array#pcd_array.rows),
    case array:get(ColumnX, Row#pcd_row.data) of
        {elem, Elem} ->
            {ok, Elem};
        {empty, _} ->
            undefined
    end.

local_index(Index, Size) ->
    {Index div Size, Index rem Size}.

global_index(Row, Column, Size) ->
    Row * Size + Column.

-spec create_new_row(Array :: pcd_array()) -> Result :: pcd_array().
create_new_row(Array) ->
    NrOfRows = Array#pcd_array.nr_of_rows,
    CreatingNewRowA = array:map(fun (Ix, _Type) ->
                                         {empty, Ix + 1}
                                end,
                                array:new(Array#pcd_array.row_size)),
    NewRowA = array:set(Array#pcd_array.row_size - 1, {empty, last}, CreatingNewRowA),
    NewRow = #pcd_row{data = NewRowA,
                      first_empty_slot = 0,
                      nr_of_empty_slots = Array#pcd_array.row_size},
    RowsWithEmptySlots = [NrOfRows | Array#pcd_array.rows_with_empty_slots],
    Array#pcd_array{nr_of_rows = NrOfRows + 1,
                    rows = array:set(NrOfRows, NewRow, Array#pcd_array.rows),
                    rows_with_empty_slots = RowsWithEmptySlots}.

create_new_array(Owner, Id, Persistent, Size, DBModule) ->
    create_new_row(#pcd_array{persistent = Persistent,
                              nr_of_rows = 0,
                              row_size = Size,
                              rows = array:new(),
                              id = Id,
                              owner_of_db = Owner,
                              db_module = DBModule}).

maybe_load_from_db(Owner, Id, Size, DBModule) ->
    case DBModule:fetch(?PCD_ARRAYS_BUCKET(Owner),
                        Id,
                        ?MODULE,
                        Owner) of
        {ok, Value} ->
            load_rows(Value#pcd_array{rows = array:new()}, 0);
        {error, notfound} ->
            Array = create_new_array(Owner, Id, true, Size, DBModule),
            update_array_db(update_chunk_db(Array, 0));
        Else ->
            lager:error("Loading Cache: ~p", [Else]),
            Else
    end.

load_rows(Array, RowNr) ->
    case load_single_row(Array#pcd_array.owner_of_db,
                         Array#pcd_array.id,
                         RowNr,
                         ?PCD_ARRAY_DB(Array)) of
        {ok, Value} ->
            NewArray = add_loaded_row(Array, Value),
            load_rows(NewArray, RowNr + 1);
        {error, notfound} ->
            Array;
        Else ->
            lager:info("Load cache Else: ~p", [Else]),
            Else
    end.

add_loaded_row(Array, Row) ->
    NrOfRows = Array#pcd_array.nr_of_rows,
    EmptyRows = case Row#pcd_row.nr_of_empty_slots of
                    0 ->
                        Array#pcd_array.rows_with_empty_slots;
                    _ ->
                        [NrOfRows | Array#pcd_array.rows_with_empty_slots]
                end,
    NewRows = array:set(NrOfRows, Row, Array#pcd_array.rows),
%    io:format("~p~nOK~p", [Array, Row]),
    NrOfElems = Array#pcd_array.nr_of_elems +
                Array#pcd_array.row_size -
                Row#pcd_row.nr_of_empty_slots,
    Array#pcd_array{nr_of_rows = NrOfRows + 1,
                    rows_with_empty_slots = EmptyRows,
                    rows = NewRows,
                    nr_of_elems = NrOfElems}.

delete_chunks(Array) ->
    delete_chunks(Array, Array#pcd_array.nr_of_rows).
delete_chunks(Array, 0) ->
    ?PCD_ARRAY_DB(Array):close();
delete_chunks(Array, N) ->
    case ?PCD_ARRAY_DB(Array):delete_keep(?PCD_ARRAYS_CHUNKS_BUCKET(Array#pcd_array.owner_of_db),
                                          ?PCD_ARRAY_KEY(Array#pcd_array.id, N - 1),
                                          ?MODULE,
                                          Array#pcd_array.owner_of_db) of
        ok ->
            delete_chunks(Array, N - 1);
        Else ->
            lager:error("~p when deleting chunk: ~p",
                        [Else, ?PCD_ARRAY_KEY(Array#pcd_array.id, N)]),
            delete_chunks(Array, N - 1)
    end.

update_array_db(Array) ->
    case Array#pcd_array.persistent of
        true ->
            case ?PCD_ARRAY_DB(Array):store(?PCD_ARRAYS_BUCKET(Array#pcd_array.owner_of_db),
                                            Array#pcd_array.id,
                                            Array,
                                            ?MODULE,
                                            Array#pcd_array.owner_of_db) of
                ok ->
                    Array
            end;
        false ->
            Array
    end.

update_chunk_db(Array, RowX) ->
    update_chunk_db(Array, RowX, array:get(RowX, Array#pcd_array.rows)).

update_chunk_db(Array, RowX, Row) ->
    NewRow = Row#pcd_row{dirty = false},
    NewArray = Array#pcd_array{rows = array:set(RowX, NewRow, Array#pcd_array.rows)},
    case NewArray#pcd_array.persistent of
        true ->
            case ?PCD_ARRAY_DB(NewArray):store(?PCD_ARRAYS_CHUNKS_BUCKET(NewArray#pcd_array.owner_of_db),
                                            ?PCD_ARRAY_KEY(NewArray#pcd_array.id, RowX),
                                            NewRow,
                                            ?MODULE,
                                            NewArray#pcd_array.owner_of_db) of
                ok ->
                    NewArray;
                Else ->
                    Else
            end;
        false ->
            NewArray
    end.

load_single_row(Owner, Id, ChunkNr, DBModule) ->
    DBModule:fetch(?PCD_ARRAYS_CHUNKS_BUCKET(Owner),
                   ?PCD_ARRAY_KEY(Id, ChunkNr),
                   ?MODULE,
                   Owner).

-spec check_health(pcd_array()) -> ok | error.
check_health(Array) ->
    %check empty slots.
    check_empty_slots(Array).

%%%%%%%%%%%%%%%%%%%%%%%

check_empty_slots(Array) ->
    FullRows = lists:seq(0, Array#pcd_array.nr_of_rows - 1) --
                   Array#pcd_array.rows_with_empty_slots,
    check_full_rows(Array, FullRows) and
        check_partly_filled_rows(Array, Array#pcd_array.rows_with_empty_slots).

check_full_rows(_, []) -> true;
check_full_rows(Array, [Row | Rest]) ->
    RowToCheck = array:get(Row, Array#pcd_array.rows),
    case (RowToCheck#pcd_row.nr_of_empty_slots =:= 0) and
         (RowToCheck#pcd_row.first_empty_slot =:= last) and
%         (RowToCheck#pcd_row.dirty =:= false) and
         check_if_row_is_full(RowToCheck, Array#pcd_array.row_size - 1) of
        true ->
            check_full_rows(Array, Rest);
        Else ->
            lager:warning("Not Empty Row:~p", [RowToCheck]),
            Else
    end.

check_if_row_is_full(_, -1) -> true;
check_if_row_is_full(Row, Ix) ->
    case array:get(Ix, Row#pcd_row.data) of
        {empty, _} ->
            lager:warning("Found empty elem at: ~p, ~w", [Row, Ix]),
            false;
        {elem, _} ->
            check_if_row_is_full(Row, Ix - 1)
    end.

check_partly_filled_rows(_, []) -> true;
check_partly_filled_rows(Array, [RowX | Rest]) ->
    RowToCheck = array:get(RowX, Array#pcd_array.rows),
    EmptyCount = RowToCheck#pcd_row.nr_of_empty_slots,
    case count_empty_slots(RowToCheck#pcd_row.data, RowToCheck#pcd_row.first_empty_slot, 0) of
        EmptyCount ->
            check_partly_filled_rows(Array, Rest);
        _ ->
            false
    end.
count_empty_slots(_, last, Count) ->
    Count;
count_empty_slots(ValueArray, Ix, Count) ->
    case array:get(Ix, ValueArray) of
        {empty, NextX} ->
            count_empty_slots(ValueArray, NextX, Count + 1);
        _ ->
            false
    end.

t_new() ->
    load(pcd, <<"TESTCACHE">>).

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
    get_elem(N, A1).

populate(Cache, 0) ->
    Cache;
populate(Cache, N) ->
    _V = {_Ix, C1} = pcd_array:add_elem({N, "maeslkdjf;l ajsdf; j;asdfj ;slajdf l;jsad; ljelement"}, Cache),
    populate(C1, N - 1).

