%% @author zsoci
%% @doc Persisten Cache for messages
%% Implements a chunked list of elements where the last couple of elements
%% are in memory in an array:array().
%% The rest is stored in RIAK in a set data type using chunks.
%% This data structure shall be used where there are almost no deletions from
%% the set. The implementation assumes to get 99.99% of writes.
%% It caches and saves the new elements into a RIAK set
%% and when the cache gets full it creates a new one.
%% @todo Add description to cu_pcd_list.

-module(pcd_list).

-behavior(pcd).

-include("pcd_common.hrl").
-include("pcd.hrl").

-compile({parse_transform, ejson_trans}).

-json_opt({type_field, [pcd_list, chunk_key]}).

-json({pcd_list,
       {number, "cache_size", [{default, 0}]},
       {number, "nr_of_chunks", [{default, 0}]},
       skip,
       {boolean, "persistent"},
       {binary, "id"},
       skip,
       {skip, [{default, none}]},
       {atom, "owner_of_db"},
       {atom, "db_module", [{default, pcd_db_riak}]}}).

-json({chunk_key,
       {binary, "id"},
       {number, "chunk_nr"}}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([load/5,
         load/0,
         load/2,
         load/3,
         load/4,
         add_elem/2,
         add_elem/3,
         get_elem/2,
         last_index/1,
         first_index/1,
         next_index/2,
         prev_index/2,
         delete/1]).

-export([to_json/1,
         from_json/1]).

-export([t_new/0,
         t_delete/1,
         populate/2,
         t_add/1,
         t_add/0,
         t_get/1
        ]).

%% load(Owner, ID)
%% load(Owner, ID, Persistent)
%% load(Owner, ID, Persistent, Size)
%% @doc
%% Load or create a new cache data structure
%% Owner :: atom to retrieve riak_db connection from application environment
%% Id :: Unique Id used as the key of the set in RIAK
%% Persistent :: true if we want to have the pcd_list persistent
%% Size :: size of the cache
%% @end
-spec load(Owner, Id, Persistent, Size, DBModule) -> Result when
          Owner :: atom(),
          Id :: binary(),
          Persistent :: boolean(),
          Size :: pos_integer(),
          DBModule :: atom(),
          Result :: pcd_list()
                  | {error, term()}.
load(Owner, Id, Persistent, Size, DBModule) ->
    case Persistent of
        true ->
            maybe_load_from_db(Owner, Id, Size, DBModule);
        false ->
            create_new_cache(Owner, Id, false, Size, DBModule)
    end.

load(Owner, Id, Persistent, RowSize) ->
    load(Owner, Id, Persistent, RowSize, ?PCD_DEFAULT_DB_MODULE).
load(Owner, Id, Persistent) ->
    load(Owner, Id, Persistent, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).
load(Owner, Id) ->
    load(Owner, Id, true, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).
load() ->
    load(undefined, <<"undefined">>, false, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).

delete(Cache) ->
    case Cache#pcd_list.persistent of
        true ->
            case ?PCD_LIST_DB(Cache):delete(?PCD_LISTS_BUCKET(Cache#pcd_list.owner_of_db),
                                   Cache#pcd_list.id,
                                   ?MODULE,
                                   Cache#pcd_list.owner_of_db) of
                ok ->
                    delete_chunks(Cache);
                Else ->
                    lager:error("~p. Could not delete cache ~p", [Else, Cache]),
                    Else
            end;
        false ->
            ok
    end.

-spec add_elem(Element :: term(), Cache :: pcd_list()) -> Result when
          Result :: {ok, non_neg_integer(), pcd_list()}
                  | {error, term()}.
add_elem(Element, Cache) ->
    case Cache#pcd_list.cached_data#chunk.next_empty <
             Cache#pcd_list.cache_size of
        true ->
            Index = Cache#pcd_list.cached_data#chunk.next_empty,
            Data = #chunk{next_empty =  Index + 1,
                          elems = array:set(Index, {elem, Element},
                                            Cache#pcd_list.cached_data#chunk.elems)},
            NewCache = Cache#pcd_list{cached_data = Data},
            GlobalIndex = (NewCache#pcd_list.nr_of_chunks - 1) *
                              NewCache#pcd_list.cache_size +
                              Index,
            check_and_update(NewCache, GlobalIndex);
        false ->
            case Cache#pcd_list.persistent of
                true ->
                    add_elem(Element, update_cache(create_new_chunk(Cache)));
                false ->
                    add_elem(Element, create_new_chunk(Cache))
            end
    end.

-spec add_elem(Element :: term(), Cache :: pcd_list(), Params :: term()) -> Result when
          Result :: {non_neg_integer(), pcd_list()}
                  | {error, term()}.
add_elem(Element, Cache, _) ->
    add_elem(Element, Cache).

check_and_update(NewCache, GlobalIndex) ->
    case NewCache#pcd_list.persistent of
        true ->
            case update_chunk(NewCache) of
                {error, _Reason} = R ->
                    R;
                UpdateCache ->
                    {ok, GlobalIndex, UpdateCache}
            end;
        false ->
            {ok, GlobalIndex, NewCache}
    end.

get_elem(GlobalIndex, Cache) ->
    case is_cached(GlobalIndex, Cache) of
        true ->
            case array:get(local_index(GlobalIndex,
                                       Cache#pcd_list.cache_size,
                                       Cache#pcd_list.nr_of_chunks),
                       Cache#pcd_list.cached_data#chunk.elems) of
                undefined ->
                    {undefined, Cache};
                {elem, Value} ->
                    {ok, Value, Cache}
            end;
        false ->
            case Cache#pcd_list.persistent of
                true ->
                    try_load_element_cache(GlobalIndex, Cache);
                false ->
                    undefined
            end
    end.

try_load_element_cache(GlobalIndex, Cache) ->
    case load_element_cache(GlobalIndex, Cache) of
        {error, Reason} ->
            {error, Reason};
        NewCache ->
            case array:get(local_index(GlobalIndex,
                                       NewCache#pcd_list.cache_size,
                                       NewCache#pcd_list.interim_chunk_nr),
                           NewCache#pcd_list.interim_data#chunk.elems) of
                undefined ->
                    {undefined, NewCache};
                {elem, Value} ->
                    {ok, Value, NewCache}
            end
    end.

last_index(Cache) ->
    Cache#pcd_list.cache_size * (Cache#pcd_list.nr_of_chunks - 1) +
                     Cache#pcd_list.cached_data#chunk.next_empty - 1.

first_index(Cache) ->
    case Cache#pcd_list.persistent of
        true ->
            0;
        false ->
            (Cache#pcd_list.nr_of_chunks - 1) * Cache#pcd_list.cache_size
    end.
%% ====================================================================
%% Internal functions
%% ====================================================================

is_cached(Index, Cache) ->
    Cache#pcd_list.nr_of_chunks =:= global_chunk_nr(Index, Cache).

global_chunk_nr(Index, Cache) ->
    Index div Cache#pcd_list.cache_size + 1.

local_index(Index, Size, ChunkNr) ->
    Index - Size * (ChunkNr - 1).

create_new_chunk(Cache) ->
    InterimChunk = Cache#pcd_list.cached_data,
    InterimChunkNr = Cache#pcd_list.nr_of_chunks,
    Cache#pcd_list{nr_of_chunks = Cache#pcd_list.nr_of_chunks + 1,
                 cached_data = #chunk{elems = array:new(Cache#pcd_list.cache_size)},
                 interim_data = InterimChunk,
                 interim_chunk_nr = InterimChunkNr}.

create_new_cache(Owner, Id, Persistent, Size, DBModule) ->
    create_new_chunk(#pcd_list{persistent = Persistent,
                             nr_of_chunks = 0,
                             cache_size = Size,
                             id = Id,
                             owner_of_db = Owner,
                             db_module = DBModule}).

%% maybe_load_from_db(Owner, Id, Size, DBModule) when is_binary(Owner) ->
%%     maybe_load_from_db(binary_to_atom(Owner, utf8), Id, Size, DBModule);
maybe_load_from_db(Owner, Id, Size, DBModule) ->
    case DBModule:fetch(?PCD_LISTS_BUCKET(Owner),
                          Id,
                          ?MODULE,
                          Owner) of
        {ok, Value} ->
            load_chunks(Value);
        {error, notfound} ->
            Cache = create_new_cache(Owner, Id, true, Size, DBModule),
            update_cache(maybe_orphaned_chunks(Cache));
        Else ->
            lager:error("Loading Cache: ~p", [Else]),
            Else
    end.

load_chunks(Cache) ->
%    lager:info("CACHE:~p", [Cache]),
    case load_single_chunk(Cache#pcd_list.owner_of_db,
                           Cache#pcd_list.id,
                           Cache#pcd_list.nr_of_chunks,
                           ?PCD_LIST_DB(Cache)) of
        {ok, undefined} ->
            lager:error("Inconsistent data. Recreating as new. ~p", [Cache]),
            delete(Cache),
            load(Cache#pcd_list.owner_of_db,
                 Cache#pcd_list.id,
                 Cache#pcd_list.persistent,
                 Cache#pcd_list.cache_size,
                 Cache#pcd_list.db_module);
        {ok, Value} ->
            update_cache(maybe_orphaned_chunks(Cache#pcd_list{cached_data = Value}));
        {error, notfound} ->
            update_cache(maybe_orphaned_chunks(Cache));
        Else ->
            lager:info("Load cache Else: ~p", [Else]),
            Else
    end.

delete_chunks(Cache) ->
    delete_chunks(Cache, Cache#pcd_list.nr_of_chunks).
delete_chunks(Cache, 0) ->
    ?PCD_LIST_DB(Cache):close();
delete_chunks(Cache, N) ->
    case ?PCD_LIST_DB(Cache):delete_keep(?PCD_LISTS_CHUNKS_BUCKET(Cache#pcd_list.owner_of_db),
                                ?PCD_CHUNK_KEY(Cache#pcd_list.id, N),
                                ?MODULE,
                                Cache#pcd_list.owner_of_db) of
        ok ->
            delete_chunks(Cache, N - 1);
        Else ->
            lager:error("~p when deleting chunk: ~p",
                        [Else, ?PCD_CHUNK_KEY(Cache#pcd_list.id, N)]),
            delete_chunks(Cache, N - 1)
    end.

maybe_orphaned_chunks(Cache) ->
    try_load_next_chunks(Cache, Cache#pcd_list.nr_of_chunks, Cache#pcd_list.cached_data).

try_load_next_chunks(Cache, ChunkNr, _LastChunk) ->
    case load_single_chunk(Cache#pcd_list.owner_of_db,
                           Cache#pcd_list.id,
                           ChunkNr + 1,
                           ?PCD_LIST_DB(Cache)) of
        {ok, Value} ->
            maybe_orphaned_chunks(Cache#pcd_list{nr_of_chunks = ChunkNr + 1,
                                               cached_data = Value});
        {error, notfound} ->
            Cache;
        Else ->
            Else
    end.

load_element_cache(Index, Cache) ->
    case Cache#pcd_list.interim_chunk_nr =:= global_chunk_nr(Index, Cache) of
        true ->
            Cache;
        false ->
            ChunkNr = global_chunk_nr(Index, Cache),
            case load_single_chunk(Cache#pcd_list.owner_of_db,
                                   Cache#pcd_list.id,
                                   ChunkNr,
                                   ?PCD_LIST_DB(Cache)) of
                {ok, Value} ->
                    Cache#pcd_list{interim_chunk_nr = ChunkNr,
                                 interim_data = Value};
                Else ->
                    Else
            end
    end.

update_cache(Cache) ->
    case (Cache#pcd_list.db_module):store(?PCD_LISTS_BUCKET(Cache#pcd_list.owner_of_db),
                          Cache#pcd_list.id,
                          Cache,
                          ?MODULE,
                          Cache#pcd_list.owner_of_db) of
        ok ->
            update_chunk(Cache)
    end.

update_chunk(Cache) ->
    case ?PCD_LIST_DB(Cache):store(?PCD_LISTS_CHUNKS_BUCKET(Cache#pcd_list.owner_of_db),
                          ?PCD_CHUNK_KEY(Cache#pcd_list.id,
                                        Cache#pcd_list.nr_of_chunks),
                          Cache#pcd_list.cached_data,
                          ?MODULE,
                          Cache#pcd_list.owner_of_db) of
        ok ->
            Cache;
        Else ->
            Else
    end.

load_single_chunk(Owner, Id, ChunkNr, DBModule) ->
    DBModule:fetch(?PCD_LISTS_CHUNKS_BUCKET(Owner),
                     ?PCD_CHUNK_KEY(Id, ChunkNr),
                     ?MODULE,
                     Owner).

next_index(Array, Index) ->
    {ok, Index + 1, Array}.

prev_index(Array, Index) ->
    {ok, Index - 1, Array}.
%%%%%%%%%%%%%%%%%%%%%%%
t_new() ->
    pcd:new(pcd_list, test, <<"TESTCACHE">>).

t_delete(C) ->
    delete(C).

t_add() ->
    t_add(10).

t_add(N) ->
    A1 = pcd_list:load(test, <<"TESTCACHE">>, true, 100),
    {Time, Value} =timer:tc(?MODULE, populate, [A1, N]),
    io:format("taken:~p~nSize:~p", [Time/1000000, erts_debug:flat_size(Value)]),
    Value.

t_get(N) ->
    A1 = pcd_list:load(test, <<"TESTCACHE">>),
    get_elem(N, A1).

populate(Cache, 0) ->
    Cache;
populate(Cache, N) ->
    _V = {_Ix, C1} = pcd_list:add_elem({N, "maeslkdjf;l ajsdf; j;asdfj ;slajdf l;jsad; ljelement"}, Cache),
%    io:format("Added:~p~n", [V]),
    populate(C1, N - 1).

