%% @author zsoci
%% @doc Persisten Cache for messages
%% Implements a chunked list of elements where the last couple of elements
%% are in memory in an array:array().
%% The rest is stored in RIAK in a set data type using chunks.
%% This data structure shall be used where there are almost no deletions from
%% the set. The implementation assumes to get 99.99% of writes.
%% It caches and saves the new elements into a RIAK set
%% and when the cache gets full it creates a new one.
%% @todo Add description to cu_pc_list.

-module(pcd_list).
-compile({parse_transform, ejson_trans}).

-include("pcd_list.hrl").

-json_opt({type_field,[pc_list, chunk_key]}).

-json({pc_list,
       {number, "cache_size", [{default, 0}]},
       {number, "nr_of_chunks", [{default, 0}]},
       skip,
       {boolean, "persistent"},
       {binary, "id"},
       skip,
       {skip, [{default, none}]},
       {atom, "owner_of_db"}}).

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
         add_elem/2,
         get_elem/2,
         last_index/1,
         first_index/1,
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
%% Persistent :: true if we want to have the pc_list persistent
%% Size :: size of the cache
%% @end
-spec load(Owner, Id,Persistent, Size, DBModule) -> Result when
          Owner :: atom(),
          Id :: binary(),
          Persistent :: boolean(),
          Size :: pos_integer(),
          DBModule :: atom(),
          Result :: pc_list()
                  | {error, term()}.
load(Owner, Id, Persistent, Size, DBModule) ->
    case Persistent of
        true ->
            maybe_load_from_db(Owner, Id, Size, DBModule);
        false ->
            create_new_cache(Owner, Id, false, Size, DBModule)
    end.
load(Owner, Id, Persistent, Size) ->
    load(Owner, Id, Persistent, Size, ?PCD_DEFAULT_DB_MODULE).
load(Owner, Id, Persistent) ->
    load(Owner, Id, Persistent, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).
load(Owner, Id) ->
    load(Owner, Id, true, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).
load() ->
    load(undefined, <<"undefined">>, false, ?PCD_DEFAULT_LIST_SIZE, ?PCD_DEFAULT_DB_MODULE).

delete(Cache) ->
    case Cache#pc_list.persistent of
        true ->
            case ?PCD_DB(Cache):delete(?PCD_LISTS_BUCKET(Cache#pc_list.owner_of_db),
                                   Cache#pc_list.id,
                                   ?MODULE,
                                   Cache#pc_list.owner_of_db) of
                ok ->
                    delete_chunks(Cache);
                Else ->
                    lager:error("~p. Could not delete cache ~p", [Else, Cache]),
                    Else
            end;
        false ->
            ok
    end.

-spec add_elem(Element :: term(), Cache :: pc_list()) -> Result when
          Result :: {non_neg_integer(), pc_list()}
                  | {error, term()}.
add_elem(Element, Cache) ->
    case Cache#pc_list.cached_data#chunk.next_empty <
             Cache#pc_list.cache_size of
        true ->
            Index = Cache#pc_list.cached_data#chunk.next_empty,
            Data = #chunk{next_empty =  Index + 1,
                          elems = array:set(Index, Element,
                                            Cache#pc_list.cached_data#chunk.elems)},
            NewCache = Cache#pc_list{cached_data = Data},
            GlobalIndex = (NewCache#pc_list.nr_of_chunks - 1) *
                              NewCache#pc_list.cache_size +
                              Index, 
            case NewCache#pc_list.persistent of
                true ->
                    case update_chunk(Cache#pc_list{cached_data = Data}) of
                        {error, _Reason} = R ->
                            R;
                        UpdateCache ->
                            {GlobalIndex, UpdateCache}
                    end;
                false ->
                    {GlobalIndex, NewCache}
            end;
        false ->
            case Cache#pc_list.persistent of
                true ->
                    add_elem(Element, update_cache(create_new_chunk(Cache)));
                false ->
                    add_elem(Element, create_new_chunk(Cache))
            end
    end.

get_elem(GlobalIndex, Cache) ->
    case is_cached(GlobalIndex, Cache) of
        true ->
            {array:get(local_index(GlobalIndex,
                                   Cache#pc_list.cache_size,
                                   Cache#pc_list.nr_of_chunks),
                       Cache#pc_list.cached_data#chunk.elems),
             Cache};
        false ->
            case Cache#pc_list.persistent of
                true ->
                    case load_element_cache(GlobalIndex, Cache) of
                        {error, _} ->
                            {undefined, Cache};
                        NewCache ->
                            {array:get(local_index(GlobalIndex,
                                                   NewCache#pc_list.cache_size,
                                                   NewCache#pc_list.interim_chunk_nr),
                                       NewCache#pc_list.interim_data#chunk.elems),
                             NewCache}
                    end;
                false ->
                    {undefined, Cache}
            end
    end.

last_index(Cache) ->
    Cache#pc_list.cache_size * (Cache#pc_list.nr_of_chunks - 1) +
                     Cache#pc_list.cached_data#chunk.next_empty - 1.

first_index(Cache) ->
    case Cache#pc_list.persistent of
        true ->
            0;
        false ->
            (Cache#pc_list.nr_of_chunks - 1) * Cache#pc_list.cache_size
    end.
%% ====================================================================
%% Internal functions
%% ====================================================================

is_cached(Index, Cache) ->
    Cache#pc_list.nr_of_chunks =:= global_chunk_nr(Index, Cache).

global_chunk_nr(Index, Cache) ->
    Index div Cache#pc_list.cache_size + 1.

local_index(Index, Size, ChunkNr) ->
    Index - Size * (ChunkNr - 1).
    
create_new_chunk(Cache) ->
    InterimChunk = Cache#pc_list.cached_data,
    InterimChunkNr = Cache#pc_list.nr_of_chunks,
    Cache#pc_list{nr_of_chunks = Cache#pc_list.nr_of_chunks + 1,
                 cached_data = #chunk{elems = array:new(Cache#pc_list.cache_size)},
                 interim_data = InterimChunk,
                 interim_chunk_nr = InterimChunkNr}.

create_new_cache(Owner, Id, Persistent, Size, DBModule) ->
    create_new_chunk(#pc_list{persistent = Persistent,
                             nr_of_chunks = 0,
                             cache_size = Size,
                             id = Id,
                             owner_of_db = Owner,
                             db_module = DBModule}).

maybe_load_from_db(Owner, Id, Size, DBModule) when is_binary(Owner) ->
    maybe_load_from_db(binary_to_atom(Owner, utf8), Id, Size, DBModule);
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
    case load_single_chunk(Cache#pc_list.owner_of_db,
                           Cache#pc_list.id,
                           Cache#pc_list.nr_of_chunks,
                           ?PCD_DB(Cache)) of
        {ok, Value} ->
            update_cache(maybe_orphaned_chunks(Cache#pc_list{cached_data = Value}));
        {error, notfound} ->
            update_cache(maybe_orphaned_chunks(Cache));
        Else ->
            lager:info("Load cache Else: ~p",[Else]),
            Else
    end.

delete_chunks(Cache) ->
    delete_chunks(Cache, Cache#pc_list.nr_of_chunks).
delete_chunks(Cache, 0) ->
    ?PCD_DB(Cache):close();
delete_chunks(Cache, N) ->
    case ?PCD_DB(Cache):delete_keep(?PCD_LISTS_CHUNKS_BUCKET(Cache#pc_list.owner_of_db),
                                ?PCD_CHUNK_KEY(Cache#pc_list.id, N),
                                ?MODULE,
                                Cache#pc_list.owner_of_db) of
        ok ->
            delete_chunks(Cache, N - 1);
        Else ->
            lager:error("~p when deleting chunk: ~p",
                        [Else, ?PCD_CHUNK_KEY(Cache#pc_list.id, N)]),
            delete_chunks(Cache, N - 1)
    end.

maybe_orphaned_chunks(Cache) ->
    try_load_next_chunks(Cache, Cache#pc_list.nr_of_chunks, Cache#pc_list.cached_data).

try_load_next_chunks(Cache, ChunkNr, _LastChunk) ->
    case load_single_chunk(Cache#pc_list.owner_of_db,
                           Cache#pc_list.id,
                           ChunkNr + 1,
                           ?PCD_DB(Cache)) of
        {ok, Value} ->
            maybe_orphaned_chunks(Cache#pc_list{nr_of_chunks = ChunkNr + 1,
                                               cached_data = Value});
        {error, notfound} ->
            Cache;
        Else ->
            Else
    end.

load_element_cache(Index, Cache) ->
    case Cache#pc_list.interim_chunk_nr =:= global_chunk_nr(Index, Cache) of
        true ->
            Cache;
        false ->
            ChunkNr = global_chunk_nr(Index, Cache),
            case load_single_chunk(Cache#pc_list.owner_of_db,
                                   Cache#pc_list.id,
                                   ChunkNr,
                                   ?PCD_DB(Cache)) of
                {ok, Value} ->
                    Cache#pc_list{interim_chunk_nr = ChunkNr,
                                 interim_data = Value};
                Else ->
                    Else
            end
    end.

update_cache(Cache) ->
    case (Cache#pc_list.db_module):store(?PCD_LISTS_BUCKET(Cache#pc_list.owner_of_db),
                          Cache#pc_list.id,
                          Cache,
                          ?MODULE,
                          Cache#pc_list.owner_of_db) of
        ok ->
            update_chunk(Cache)
    end.

update_chunk(Cache) ->
    case ?PCD_DB(Cache):store(?PCD_LISTS_CHUNKS_BUCKET(Cache#pc_list.owner_of_db),
                          ?PCD_CHUNK_KEY(Cache#pc_list.id,
                                        Cache#pc_list.nr_of_chunks),
                          Cache#pc_list.cached_data,
                          ?MODULE,
                          Cache#pc_list.owner_of_db) of
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

%%%%%%%%%%%%%%%%%%%%%%%
t_new() ->
    load(test, <<"TESTCACHE">>).

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
%    io:format("Added:~p~n",[V]),
    populate(C1, N - 1).    