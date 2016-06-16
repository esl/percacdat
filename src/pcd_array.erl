%% @author zsoci
%% @doc @todo Add description to PCD_pcarray.

-module(pcd_array).

-behavior(pcd).
-include("pcd_common.hrl").
-include("pcd_array.hrl").

-compile({parse_transform, ejson_trans}).
-json_opt({type_field, [data, chunk_key]}).

-json({data,
       {number, "row_size", [{default, ?PCD_DEFAULT_ROW_SIZE}]},
       {skip, [{default, 0}]},  % nr of rows is calculated by collecting the chunks
       skip,  % rows as an array
       {skip, [{default, []}]},  % rows_with_empty_slots
       {boolean, "persistent"},
       {binary, "id"},
       {atom, "owner_of_db"},
       {atom, "db_module", [{default, pcd_db_riak}]},
       skip,
       {skip, [{default, 0}]},
       {skip, [{default, []}]}, % delayed row numbers
       skip % delayed row params
}). % nr_of_elems

-json({chunk_key,
       {binary, "id"},
       {number, "chunk_nr"}}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([load/5,
         load/4,
         load/3,
         load/2,
         load/0,
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
         first_index/1,
         last_index/1,
         check_health/1,
         next_index/2,
         prev_index/2
        ]).

-export([to_json/1,
         from_json/1]).

-export([get_local_index/2]).

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
          Result :: {ok, data()}
                  | {error, term()}.
load(Owner, Id, Persistent, Size, DBModule) ->
    case Persistent of
        true ->
            maybe_load_from_db(Owner, Id, Size, DBModule);
        false ->
            {ok, create_new_array(Owner, Id, false, Size, DBModule)}
    end.

load(Owner, Id, Persistent, RowSize) ->
    load(Owner, Id, Persistent, RowSize, ?PCD_DEFAULT_DB_MODULE).
load(Owner, Id, Persistent) ->
    load(Owner, Id, Persistent, ?PCD_DEFAULT_ROW_SIZE, ?PCD_DEFAULT_DB_MODULE).
load(Owner, Id) ->
    load(Owner, Id, true, ?PCD_DEFAULT_ROW_SIZE, ?PCD_DEFAULT_DB_MODULE).
load() ->
    load(undefined, <<"undefined">>, false, ?PCD_DEFAULT_ROW_SIZE, ?PCD_DEFAULT_DB_MODULE).

-spec set_delayed_write_fun(Array :: data(),
                            Fun :: fun()) ->
                                Reply :: data().
set_delayed_write_fun(Array, Fun) ->
    Array#pcd_array{relief_fun = Fun}.

-spec get_local_index(GlobalIndex :: non_neg_integer(),
                      Array :: data()) -> {Row :: non_neg_integer(),
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

-spec add_elem(Element :: term(), Array :: data()) -> Result when
          Result :: {ok, non_neg_integer(), data()}
                  | {error, term()}.
add_elem(Element, Array) ->
    add_elem(Element, Array, undefined).

-spec add_elem(Element :: term(), Array :: data(), Params :: term()) -> Result when
          Result :: {ok, non_neg_integer(), data()}
                  | {error, term()}.
add_elem(Element, Array, Params) ->
    % find a row with empty slots first
    case Array#pcd_array.rows_with_empty_slots of
        [] ->
            % no empty slot, add a row and retry
            add_elem(Element, create_new_row(Array), Params);
        [RowX | MayKeepList] ->
            add_elem_to_row(RowX, Element, Array, MayKeepList, Params)
    end.

add_elem_to_row(RowX, Element, Array, MayKeepList, Params) ->
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
            SavedArray = update_chunk_db(Array#pcd_array{nr_of_elems = NrOfElems},
                                         RowX, NewRow, GlobalIndex, Params),
            case NewNrOfEmptySlots of
                0 ->
                    {ok, GlobalIndex, SavedArray#pcd_array{rows_with_empty_slots = MayKeepList}};
                _ ->
                    {ok, GlobalIndex, SavedArray}
            end;
        _Else ->
            {error, notempty}
    end.

-spec delete_elem(GlobalIndex :: non_neg_integer(), Array :: data()) -> Reply
    when Reply :: {ok, data()}
                | {undefined, data()}
                | {error, term()}.
delete_elem(GlobalIndex, Array) ->
    delete_elem(GlobalIndex, Array, undefined).

-spec delete_elem(GlobalIndex :: non_neg_integer(), Array :: data(), Params :: term()) -> Reply
    when Reply :: {ok, data()}
                | {undefined, data()}
                | {error, term()}.
delete_elem(GlobalIndex, Array, Params) ->
    case get_elem(GlobalIndex, Array) of
        {ok, _, _} ->
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
            {ok, update_chunk_db(Array#pcd_array{rows_with_empty_slots = RowsWithEmptySlots,
                                                 nr_of_elems = NrOfElems},
                                 RowX,
                                 NewRow,
                                 GlobalIndex,
                                 Params)};
        _ ->
            {undefined, Array}
    end.

-spec get_elem(GlobalIndex :: non_neg_integer(), Array :: data()) -> Reply
    when Reply :: {ok, Value :: term(), NewArray :: data()}
                | {error, undefined}.
get_elem(GlobalIndex, Array) ->
    {RowX, ColumnX} = local_index(GlobalIndex, Array#pcd_array.row_size),
    Row = array:get(RowX, Array#pcd_array.rows),
    case array:get(ColumnX, Row#pcd_row.data) of
        {elem, Elem} ->
            {ok, Elem, Array};
        {empty, _} ->
            {error, undefined}
    end.

local_index(Index, Size) ->
    {Index div Size, Index rem Size}.

global_index(Row, Column, Size) ->
    Row * Size + Column.

-spec create_new_row(Array :: data()) -> Result :: data().
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
                              db_module = DBModule,
                              delayed_row_params = array:new()}).
maybe_load_from_db(Owner, Id, Size, DBModule) ->
    case DBModule:fetch(?PCD_ARRAYS_BUCKET(Owner),
                        Id,
                        ?MODULE,
                        Owner) of
        {ok, Value} ->
            load_rows(Value#pcd_array{rows_with_empty_slots = [],
                                      nr_of_rows = 0,
                                      rows = array:new(),
                                      delayed_row_params = array:new()}, 0);
        {error, notfound} ->
            Array = create_new_array(Owner, Id, true, Size, DBModule),
            update_array_db(update_chunk_db(Array, 0, 0, undefined));
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
            {ok, Array};
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
    case ?PCD_ARRAY_DB(Array):store(?PCD_ARRAYS_BUCKET(Array#pcd_array.owner_of_db),
                                    Array#pcd_array.id,
                                    Array,
                                    ?MODULE,
                                    Array#pcd_array.owner_of_db) of
        ok ->
            {ok, Array};
        Else ->
            Else
    end.

update_chunk_db(Array, RowX, GlobalX, Params) ->
    update_chunk_db(Array, RowX, array:get(RowX, Array#pcd_array.rows), GlobalX, Params).

update_chunk_db(Array, RowX, Row, GlobalX, Params) ->
    case Array#pcd_array.persistent of
        true ->
            maybe_delayed_write(Array, RowX, Row, GlobalX, Params);
        false ->
            Array#pcd_array{rows = array:set(RowX, Row, Array#pcd_array.rows)}
    end.

maybe_delayed_write(Array, RowX, Row, GlobalX, Params) ->
    case Params of
        undefined ->
            NewRow = Row#pcd_row{dirty = false},
            NewArray = Array#pcd_array{rows = array:set(RowX, NewRow, Array#pcd_array.rows)},
            case ?PCD_ARRAY_DB(NewArray):store(
                   ?PCD_ARRAYS_CHUNKS_BUCKET(NewArray#pcd_array.owner_of_db),
                   ?PCD_ARRAY_KEY(NewArray#pcd_array.id, RowX),
                   NewRow,
                   ?MODULE,
                   NewArray#pcd_array.owner_of_db) of
                ok ->
                    NewArray;
                Else ->
                    Else
            end;
        _ ->
            {DParams, DRowNrList} = case array:get(RowX, Array#pcd_array.delayed_row_params) of
                                        undefined ->
                                            {[{GlobalX, Params}],
                                             [ RowX | Array#pcd_array.delayed_row_nrs ]};
                                        List ->
                                            {[{GlobalX, Params} | List ],
                                             Array#pcd_array.delayed_row_nrs}
                                    end,
            Array#pcd_array{rows = array:set(RowX, Row, Array#pcd_array.rows),
                            delayed_row_params = array:set(RowX, DParams,
                                                           Array#pcd_array.delayed_row_params),
                            delayed_row_nrs = DRowNrList}
    end.

load_single_row(Owner, Id, ChunkNr, DBModule) ->
    DBModule:fetch(?PCD_ARRAYS_CHUNKS_BUCKET(Owner),
                   ?PCD_ARRAY_KEY(Id, ChunkNr),
                   ?MODULE,
                   Owner).

-spec write(data()) -> ok | error.
write(Array) ->
    case Array#pcd_array.persistent of
        true ->
            write_chunks_delayed(Array, Array#pcd_array.delayed_row_nrs);
        _ ->
            ok
    end.

write_chunks_delayed(_, []) ->
    ok;
write_chunks_delayed(Array, [ RowX | Rest ]) ->
    write_single_chunk(Array, RowX),
    Params = array:get(RowX, Array#pcd_array.delayed_row_params),
    call_delayed_funs(Array#pcd_array.relief_fun, Params),
    NewParamArray = array:set(RowX, undefined, Array#pcd_array.delayed_row_params),
    write_chunks_delayed(Array#pcd_array{delayed_row_params = NewParamArray,
                                         delayed_row_nrs = Rest},
                         Rest).

write_single_chunk(Array, RowX) ->
    Row = array:get(RowX, Array#pcd_array.rows),
    NewRow = Row#pcd_row{dirty = false},
    NewArray = Array#pcd_array{rows = array:set(RowX, NewRow, Array#pcd_array.rows)},
    case ?PCD_ARRAY_DB(NewArray):store(
           ?PCD_ARRAYS_CHUNKS_BUCKET(NewArray#pcd_array.owner_of_db),
           ?PCD_ARRAY_KEY(NewArray#pcd_array.id, RowX),
           NewRow,
           ?MODULE,
           NewArray#pcd_array.owner_of_db) of
        ok ->
            NewArray;
        Else ->
            lager:error("Cannot Write single chunk~p", [Else]),
            Else
    end.

last_index(Array) ->
    Array#pcd_array.nr_of_elems - 1.

first_index(_) ->
    0.

-spec next_index(data(), index()) ->
          {ok, index(), data()}.
next_index(Array, Index) ->
    {ok , Index + 1, Array}.

prev_index(Array, Index) ->
    {ok, Index - 1, Array}.

call_delayed_funs(_, []) ->
    ok;
call_delayed_funs(Fun, [ {Ix, Params} | Rest ]) ->
    Fun(Ix, Params),
    call_delayed_funs(Fun, Rest).

-spec check_health(data()) -> boolean().
check_health(Array) ->
    %check empty slots.
    check_empty_slots(Array).

-spec update_elem(index(), any(), data()) ->
          {ok, data()}
        | {undefined, data()}
        | {error, any()}.
update_elem(_Index, _Elem, _Array) ->
    {error, undefined}.

-spec update_elem_in_cache(GlobalIndex :: index(), Elem :: term(), Array :: data()) ->
          Result :: {ok, data()}
              | {undefined, NewArray :: data()}
              | {error, Reason :: term()}.
update_elem_in_cache(GlobalIndex, Elem, Array) ->
    case get_elem(GlobalIndex, Array) of
        {ok, _, NewArray} ->
            {RowX, ColumnX} = local_index(GlobalIndex, NewArray#pcd_array.row_size),
            Row = array:get(RowX, NewArray#pcd_array.rows),
            NewRow = Row#pcd_row{data = array:set(ColumnX, {elem, Elem}, Row#pcd_row.data),
                                 dirty = true},
            {ok, NewArray#pcd_array{rows = array:set(RowX, NewRow, NewArray#pcd_array.rows)}};
        _ ->
            {undefined, Array}
    end.

-spec update_elem(index(), any(), data(), any()) ->
          {ok, data()}
        | {undefined, data()}
        | {error, any()}.
update_elem(_Index, _Elem, _Array, _Msg) ->
    {error, undefined}.

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
