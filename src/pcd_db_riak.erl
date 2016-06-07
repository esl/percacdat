%% @author zsolt.laky@erlang-solutions.com
%% @doc @todo Add description to psh_db_riak.
-module(pcd_db_riak).
-compile({parse_transform, lager_transform}).

-define(PCD_DEFAULT_RIAKTIMEOUT, 50000).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, terminate/1,
         fetch/4, fetch/5, fetch_keep/4,
         store/5, store/6, store_keep/5,
         get/4, update/5, update/4, update_keep/4,
         fetch_type/4, update_type/5, update_type/6,
         close/0,
         delete/4, delete_keep/4,
         delete_object/2, delete_object/3, delete_object_keep/2]).

-include_lib("riakc/include/riakc.hrl").

-spec init(Args :: term()) -> Result :: term().
init(_Args) ->
    ok.

-spec terminate(Args :: term()) -> Result :: term().
terminate(_Args) ->
    ok.
%% <h3><a name="aa">this module generates a record in the database.</a></h3>
%% additional text

%% store/5
%% <a>Stores a KV pair in a bucket and
%% close the db connection using the given converter</a>
-spec store(Bucket:: term(), Key :: term(), Value :: term(),
            Converter:: atom(), Owner :: atom()) -> Result when
          Result :: ok |
              {error, term()}.
store(Bucket, Key, Value, Converter, Owner) ->
    store(Bucket, Key, Value, false, Converter, Owner).

%% store_keep/4
%% <a>Stores a KV pair in a bucket and
%% retain the db connection using the given converter</a>
-spec store_keep(Bucket:: term(), Key :: term(), Value :: term(),
                 Converter:: atom(), Owner :: atom()) -> Result when
          Result :: ok |
              {error, term()}.
store_keep(Bucket, Key, Value, Converter, Owner) ->
    store(Bucket, Key, Value, true, Converter, Owner).

%% store/6
%% <a>Stores a KV pair in a bucket, with possibly retaining
%% db connection, using converter module to convert the key
%% and value to a given format. The converter module shall
%% implement encode_value/1 -> {"content-type:ddddd", EncodedValue}
%% encode_key(Key) -> EncodedKey and decode_value(Value)</a>
-spec store(Bucket:: term(), Key :: term(), Value :: term(),
            Keep :: boolean(), Converter :: atom(),
            Owner :: atom()) -> Result when
          Result :: ok |
              {error, term()}.
store(Bucket, Key, Value, Keep, Converter, Owner) ->
    BinKey = encode_key(Key, Converter),
    {ContentType, ContentValue} = encode_value(Value, Converter),
    case getriakpid(Owner) of
        Pid when is_pid(Pid) ->
            Reply = riakc_pb_socket:put(Pid,
                                        riakc_obj:new(Bucket,
                                                      BinKey,
                                                      ContentValue,
                                                      ContentType),
                                        [], 34),
            case Keep of
                false ->
                    close();
                _ ->
                    ok
            end,
            Reply;
        WAFIT ->
            WAFIT
    end.

%% fetch/4
%% <a>Retrieves an erlang term from Bucket with Key,
%% with converter and close db connection</a>
-spec fetch(Bucket:: term(), Key :: term(),
            Converter :: boolean() | atom(),
            Owner :: atom()) -> Result when
          Result :: {ok, Value :: term()} |
              {error, Reason :: term()}.
fetch(Bucket, Key, Converter, Owner) when is_atom(Converter) ->
    fetch(Bucket, Key, false, Converter, Owner).

%% fetch_keep/4
%% <a>Retrieves an erlang term from Bucket with Key,
%% with converter and retain db connection</a>
-spec fetch_keep(Bucket:: term(), Key :: term(),
                 Converter :: boolean() | atom(),
                 Onwer :: atom()) -> Result when
          Result :: {ok, Value :: term()} |
              {error, Reason :: term()}.
fetch_keep(Bucket, Key, Converter, Owner) when is_atom(Converter) ->
    fetch(Bucket, Key, true, Converter, Owner).

%% fetch/5
%% <a>Retrieves an erlang term from Bucket with Key,
%% with possibliy closing db connection</a>
-spec fetch(Bucket:: term(), Key :: term(), Keep :: boolean(),
            Converter:: atom(), Onwer :: atom()) -> Result when
          Result :: {ok, Value :: term()} |
              {error, Reason :: term()}.
fetch(Bucket, Key, Keep, Converter, Owner) ->
    case get(Bucket, Key, Converter, Owner) of
        {ok, Value, _Object} ->
            case Keep of
                false ->
                    close();
                _ ->
                    ok
            end,
            {ok, Value};
        {error, Reason} ->
            _ = case Reason =/= notfound of
                    true ->
                        lager:error("Riak Get error:~p", [Reason]);
                    _ ->
                        ok
                end,
            close(),
            {error, Reason}
    end.

%% update/4
%% <a>Updates a db object and stores it into the db,
%% with retaining the db connection and using
%% the specified converter module</a>
-spec update(Object :: term(), Value :: term(),
             Converter :: atom(), Owner :: atom()) -> Result when
          Result :: ok |
              {error, Reason :: term()}.
update(Object, Value, Converter, Owner) ->
    update(Object, Value, false, Converter, Owner).

%% update_keep/4
%% <a>Updates a db object and stores it into the db,
%% with retaining the db connection and using
%% the specified converter module</a>
-spec update_keep(Object :: term(), Value :: term(),
                  Converter :: atom(), Owner :: atom()) -> Result when
          Result :: ok |
              {error, Reason :: term()}.
update_keep(Object, Value, Converter, Owner) ->
    update(Object, Value, true, Converter, Owner).

%% update/5
%% <a>Updates a db object and stores it into the db,
%% with closing the db connection and using
%% the specified converter module</a>
-spec update(Object :: term(), Value :: term(), Keep :: boolean(),
             Converter :: atom(), Owner :: atom()) -> Result when
          Result :: ok |
              {error, Reason :: term()}.
update(Object, Value, Keep, Converter, Owner) ->
    Reply =
        case getriakpid(Owner) of
            Pid when is_pid(Pid) ->
                {ContentType, ContentValue} = encode_value(Value, Converter),
                riakc_pb_socket:put(Pid, riakc_obj:update_value(Object, ContentValue, ContentType));
            WAFIT ->
                _ = lager:error("Riak put error:~p", [WAFIT]),
                WAFIT
        end,
    case Keep of
        false ->
            close();
        _ ->
            ok
    end,
    Reply.

%% delete_object/2
%% <a>Deletes a db object with closing the db connection</a>
-spec delete_object(Object :: term(), Owner :: atom()) -> Result when
          Result :: ok |
              {error, Reason :: term()}.
delete_object(Object, Owner) ->
    delete_object(Object, false, Owner).

%% delete_object_keep/2
%% <a>Deletes a db object with closing the db connection</a>
-spec delete_object_keep(Object :: term(), Owner :: atom()) -> Result when
          Result :: ok |
              {error, Reason :: term()}.
delete_object_keep(Object, _Owner) ->
    delete_object(Object, false).

%% delete_object/3
%% <a>Deletes a db object with possibly closing the db connection</a>
-spec delete_object(Object :: term(), Keep :: boolean(), Owner :: atom()) -> Result when
          Result :: ok |
              {error, Reason :: term()}.
delete_object(Object, Keep, Owner) ->
    Reply =
        case getriakpid(Owner) of
            Pid when is_pid(Pid) ->
                riakc_pb_socket:delete_obj(Pid, Object);
            WAFIT ->
                _ = lager:error("Riak put error:~p", [WAFIT]),
                WAFIT
        end,
    case Keep of
        false ->
            close();
        _ ->
            ok
    end,
    Reply.

delete_keep(Bucket, Key, Converter, Owner) ->
    BinKey = encode_key(Key, Converter),
    case getriakpid(Owner) of
        Pid when is_pid(Pid) ->
            riakc_pb_socket:delete(Pid, Bucket, BinKey);
        WAFIT ->
            _ = lager:error("Riak delete error:~p", [WAFIT]),
            close(),
            WAFIT
    end.

delete(Bucket, Key, Converter, Owner) ->
    BinKey = encode_key(Key, Converter),
    case getriakpid(Owner) of
        Pid when is_pid(Pid) ->
            riakc_pb_socket:delete(Pid, Bucket, BinKey);
        WAFIT ->
            _ = lager:error("Riak delete error:~p", [WAFIT]),
            WAFIT
    end.

%% get/3
%% <a>Retrieves an erlang term and db object from Bucket with Key,
%% with retaining db connection usin a converter</a>
-spec get(Bucket:: term(), Key :: term(), Converter :: atom(),
          Owner :: atom()) -> Result when
          Result :: {ok, Value :: term(), Object :: term()} |
              {error, Reason :: term()}.
get(Bucket, Key, Converter, Owner) ->
    case getriakpid(Owner) of
        Pid when is_pid(Pid) ->
            BinKey = encode_key(Key, Converter),
            case riakc_pb_socket:get(Pid, Bucket, BinKey) of
                {ok, Object} ->
                    BinValue = riakc_obj:get_value(Object),
                    {ok, decode_value(BinValue, riakc_obj:get_content_type(Object), Converter), Object};
                {error, notfound} ->
                    {error, notfound};
                {error, Reason} ->
                    _ = lager:error("Riak Get error:~p", [Reason]),
                    {error, Reason}
            end;
        WAFIT ->
            _ = lager:error("Riak connection cannot set up. Reason:~p", [WAFIT]),
            {error, WAFIT}
    end.

fetch_type(Bucket, Key, Converter, Owner) ->
    case getriakpid(Owner) of
        Pid when is_pid(Pid) ->
            BinKey = encode_key(Key, Converter),
            riakc_pb_socket:fetch_type(Pid, Bucket, BinKey);
        WAFIT ->
            _ = lager:error("Riak connection. Reason:~p", [WAFIT]),
           {error, WAFIT}
    end.

update_type(Bucket, Key, Operation, Owner, Converter) ->
    update_type(Bucket, Key, Operation, Owner, Converter, []).

update_type(Bucket, Key, Operation, Owner, Converter, Options) ->
    case getriakpid(Owner) of
        Pid when is_pid(Pid) ->
            BinKey = encode_key(Key, Converter),
            riakc_pb_socket:update_type(Pid, Bucket, BinKey, Operation, Options);
        WAFIT ->
            _ = lager:error("Riak connection cannot be set up. Reason:~p", [WAFIT]),
           {error, WAFIT}
    end.

getriakpid(Owner) ->
    case get(riakpid) of
        undefined ->
            case riakc_pb_socket:start_link(
                   application:get_env(Owner, riak_ip, "127.0.0.1"),
                   application:get_env(Owner, riak_port, 10017),
                   []) of
                {ok, Pid} when is_pid(Pid) ->
                    put(riakpid, Pid),
                    Pid;
                WAFIT ->
                    _ = lager:error("Connecting to RIAK at(~p:~p):~p:",
                               [application:get_env(Owner, riak_ip, "127.0.0.1"),
                                application:get_env(Owner, riak_port, 10017), WAFIT]),
                    error
            end;
        Value ->
            Value
    end.

close() ->
    case get(riakpid) of
        undefined -> ok;
        Value ->
            erase(riakpid),
            riakc_pb_socket:stop(Value)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

encode_key(Key, none) -> term_to_binary(Key);
encode_key(Key, _Converter) when is_binary(Key) -> Key;
encode_key(Key, Converter) ->
    {ok, ConvertedKey} = Converter:to_json(Key),
    ConvertedKey.

%% encode_value/1
%% <a>Encode value with encoder if possible</a>
-spec encode_value(Value:: term(), Converter :: atom()) -> Result when
          Result :: {ContentType :: string(), Value :: term()}.

encode_value(Value, _Converter) when is_binary(Value) ->
    {"application/x-erlang-binary", Value};
encode_value(Value, Converter) ->
    case io_lib:printable_unicode_list(Value) of
        true ->
            {"application/text", list_to_binary(Value)};
        _ ->
            case is_tuple(Value) of
                true ->
                    encode_from_tuple(Value, Converter);
                _ ->
                    % not tuple just convert it to binary
                    {"application/x-erlang-binary", term_to_binary(Value)}
            end
    end.

encode_from_tuple(Value, Converter) ->
    try element(1, Value) of
        RecName when is_atom(RecName) ->
            case is_record(Value, RecName) of
                true ->
                    try_to_convert_to_json(Value, Converter);
                _ ->
                    % record is not in a format to convert
                    {"application/x-erlang-binary", term_to_binary(Value)}
            end;
        _ ->
            _ = lager:warning(
                "Record format is not suitable to convert. Value:~p. " ++
                "Converting term to binary",
                [Value]),
            {"application/x-erlang-binary", term_to_binary(Value)}
    catch
        A:B ->
            _ = lager:error("Exception:~p:~p"
                        "Record format is not suitable"
                        " to convert. Record:~p. "
                        "Stack:~p", [A, B, Value,
                                    erlang:get_stacktrace()]),
            {"application/x-erlang-binary", term_to_binary(Value)}
    end.


try_to_convert_to_json(Value, Converter) ->
    try
        {ok, EncodedValue} = Converter:to_json(Value),
        {"application/json", EncodedValue}
    catch
        _:_ ->
            {"application/x-erlang-binary", term_to_binary(Value)}
    end.

decode_value(BinValue, ContentType, Converter) ->
    case ContentType of
        "application/text" ->
            binary_to_list(BinValue);
        "application/x-erlang-binary" ->
            binary_to_term(BinValue);
        "application/json" ->
            {ok, Value} = Converter:from_json(BinValue),
            Value;
        ELSE ->
            _ = lager:warning("Unhandled application format:~p", [ELSE]),
            BinValue
    end.
