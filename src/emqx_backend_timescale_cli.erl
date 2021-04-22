%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午5:04
%%%-------------------------------------------------------------------

-module(emqx_backend_timescale_cli).

-export([logger_header/0]).
-include("../include/emqx_backend_timescale.hrl").
-include("../include/emqx.hrl").
-include("../include/logger.hrl").


-export([pgsql_insert/2, get_templates/0, prepare_data/2]).

-behaviour(ecpool_worker).

-export([connect/1]).

-import(proplists, [get_value/2]).


pgsql_insert(Pool, Data) ->
  ecpool:with_client(Pool,
    fun (C) ->
      [case is_list(lists:last(Params)) of
         true ->
           [epgsql:prepared_query(C, Name, Param) || Param <- Params];
         false ->
           epgsql:prepared_query(C, Name, Params)
       end
        || {Name, Params} <- Data]
    end).

get_templates() ->
  FilePath = filename:join([application:get_env(emqx, data_dir, "./"), "templates", emqx_backend_timescale]) ++ ".tmpl",
  case file:read_file(FilePath) of
    {error, Reason} ->
      begin
        logger:log(error, #{}, #{report_cb => fun (_) -> {logger_header()() ++ "Read ~p failed due to: ~p", [FilePath, posix_errno(Reason)]} end,
            mfa => {emqx_backend_timescale_cli, get_templates, 0}, line => 40})
      end,
      #{};
    {ok, <<>>} ->
      begin
        logger:log(debug, #{}, #{report_cb =>
          fun (_) -> {logger_header()() ++ "~p is empty", [FilePath]} end,
            mfa =>
            {emqx_backend_timescale_cli, get_templates, 0},
            line => 43})
      end,
      #{};
    {ok, TemplatesJson} ->
      Templates = emqx_json:decode(TemplatesJson,
        [return_maps]),
      maps:map(fun (_Topic,
          Template = #{<<"name">> := Name}) ->
        maps:put(<<"name">>,
          binary_to_list(Name),
          Template)
               end,
        Templates)
  end.

prepare_data(Message = #message{topic = Topic}, Templates) ->
  case lists:foldl(fun ({Topic0, Template}, Acc) ->
    case emqx_topic:match(Topic, Topic0) of
      true -> [Template | Acc];
      false -> Acc
    end
                   end,
    [],
    maps:to_list(Templates))
  of
    [] -> error(no_available_template);
    Templates1 ->
      Data = available_fields(Message),
      lists:foldl(fun (#{<<"name">> := Name,
        <<"param_keys">> := ParamKeys},
          Acc) ->
        [{Name, params(ParamKeys, Data)} | Acc]
                  end,
        [],
        Templates1)
  end.

connect(Opts) ->
  {ok, C} = epgsql:connect(get_value(host, Opts),
    get_value(username, Opts),
    get_value(password, Opts),
    conn_opts(Opts)),
  parse_sql(C),
  {ok, C}.

available_fields(#message{id = Id, qos = QoS, from = ClientId, headers = Headers, topic = Topic, payload = Payload, timestamp = Timestamp}) ->
  #{<<"$id">> => Id, <<"$qos">> => QoS,
    <<"$clientid">> => ClientId,
    <<"$username">> =>
    maps:get(<<"$username">>, Headers, null),
    <<"$peerhost">> =>
    host_to_str(maps:get(<<"$peerhost">>, Headers, null)),
    <<"$topic">> => Topic,
    <<"$payload">> => {Payload, try_decode(Payload)},
    <<"$timestamp">> => Timestamp}.

try_decode(Data) ->
  try emqx_json:decode(Data, [return_maps]) of
    Json -> Json
  catch
    error:Reason:Stacktrace ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header()() ++
              "Decode ~p failed due to: ~p",
              [Data, {Reason, Stacktrace}]}
          end,
            mfa => {emqx_backend_timescale_cli, try_decode, 1},
            line => 105})
      end,
      #{}
  end.

params(Keys, Data) ->
  RawParams = lists:foldr(fun (Key, Acc) ->
    case get_val(Key, Data) of
      V when Acc =:= [] ->
        case is_list(V) of
          true -> [V];
          false -> [[V]]
        end;
      V ->
        V1 = case is_list(V) of
               true -> V;
               false -> [V]
             end,
        case length(lists:last(Acc)) =:=
          length(V1)
        of
          true -> [V1 | Acc];
          false ->
            error({cannot_apply_template,
              different_length})
        end
    end
                          end,
    [],
    Keys),
  lists:foldl(fun (RawParam, []) -> RawParam;
    (RawParam, Acc) -> zip(Acc, RawParam)
              end,
    [],
    RawParams).

zip(List1, List2)
  when length(List1) =:= length(List2) ->
  zip(List1, List2, []).

zip([V1 | Rest1], [V2 | Rest2], Acc) ->
  zip(Rest1, Rest2, Acc ++ [to_list(V1) ++ to_list(V2)]);
zip([], [], Acc) -> Acc.

to_list(Data) when is_list(Data) -> Data;
to_list(Data) -> [Data].

get_val(<<"$payload">>,
    #{<<"$payload">> := {_, Payload}})
  when is_binary(Payload) orelse
  is_integer(Payload) or is_float(Payload) ->
  Payload;
get_val(<<"$payload">>,
    #{<<"$payload">> := {Raw, _}}) ->
  Raw;
get_val(Key = <<"$", Rest/binary>>, Data) ->
  try binary_to_integer(Rest) of
    _ -> error(invalid_template)
  catch
    error:_Reason ->
      case maps:get(Key, Data, undefined) of
        undefined ->
          error({invalid_template, invalid_placeholder});
        Val -> Val
      end
  end;
get_val(Key, _Data) when is_binary(Key) -> Key;
get_val([], _Data) -> error(invalid_template);
get_val([<<"$payload">>],
    #{<<"$payload">> := {_, Payload}})
  when is_binary(Payload) orelse
  is_integer(Payload) or is_float(Payload) ->
  Payload;
get_val([<<"$payload">> | Rest],
    #{<<"$payload">> := {_, Payload}})
  when is_map(Payload) or is_list(Payload) ->
  get_val_(Rest, Payload);
get_val([<<"$payload">> | _Rest],
    #{<<"$payload">> := _}) ->
  error({cannot_apply_template, invalid_data_type});
get_val([_ | _Rest], _Data) -> error(invalid_template).

get_val_([], Data) -> Data;
get_val_([<<"$", N0/binary>> | Rest], Data)
  when is_list(Data) ->
  try binary_to_integer(N0) of
    0 ->
      lists:foldr(fun (D, Acc) ->
        case get_val_(Rest, D) of
          List when is_list(List) -> List ++ Acc;
          Other -> [Other | Acc]
        end
                  end,
        [],
        Data);
    N ->
      case N > length(Data) of
        true -> error({cannot_apply_template, data_shortage});
        false -> get_val_(Rest, lists:nth(N, Data))
      end
  catch
    error:_Reason ->
      error({invalid_template, invalid_placeholder})
  end;
get_val_([<<"$", _/binary>> | _Rest], _Data) ->
  error({cannot_apply_template, cannot_fetch_data});
get_val_([Key | Rest], Data)
  when is_binary(Key), is_map(Data) ->
  case maps:get(Key, Data, undefined) of
    undefined ->
      error({cannot_apply_template, cannot_fetch_data});
    Val -> get_val_(Rest, Val)
  end;
get_val_([_ | _Rest], _Data) ->
  error({cannot_apply_template, cannot_fetch_data}).

conn_opts(Opts) -> conn_opts(Opts, []).

conn_opts([], Acc) -> Acc;
conn_opts([Opt = {database, _} | Opts], Acc) ->
  conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {ssl, _} | Opts], Acc) ->
  conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {port, _} | Opts], Acc) ->
  conn_opts(Opts, [Opt | Acc]);
conn_opts([Opt = {timeout, _} | Opts], Acc) ->
  conn_opts(Opts, [Opt | Acc]);
conn_opts([_Opt | Opts], Acc) -> conn_opts(Opts, Acc).

parse_sql(C) ->
  Templates = emqx_backend_timescale_cli:get_templates(),
  lists:foreach(fun ({_Topic,
    #{<<"name">> := Name, <<"sql">> := SQL,
      <<"param_keys">> := ParamKeys}})
    when is_list(Name), is_binary(SQL),
    is_list(ParamKeys) ->
    ok = check_param_keys(ParamKeys),
    {ok, _} = epgsql:parse(C,
      Name,
      binary_to_list(SQL),
      [])
                end,
    maps:to_list(Templates)).

check_param_keys([ParamKey | Rest]) ->
  case check_param_key(ParamKey) of
    ok -> check_param_keys(Rest);
    {error, Reason} -> {error, Reason}
  end;
check_param_keys([]) -> ok.

check_param_key(ParamKey = <<"$", _Rest/binary>>) ->
  case lists:member(ParamKey,
    [<<"$id">>,
      <<"$qos">>,
      <<"$clientid">>,
      <<"$username">>,
      <<"$peerhost">>,
      <<"$topic">>,
      <<"$payload">>,
      <<"$timestamp">>])
  of
    true -> ok;
    false -> {error, {invalid_template, invalid_param_key}}
  end;
check_param_key(ParamKey) when is_binary(ParamKey) ->
  ok;
check_param_key([]) ->
  {error, {invalid_template, missing_param_key}};
check_param_key([<<"$payload">> | Rest]) ->
  try lists:foreach(fun (<<"$", N/binary>>) ->
    try binary_to_integer(N) of
      _ -> ok
    catch
      error:_Reason ->
        error({invalid_template,
          invalid_placeholder})
    end;
    (ParamKey) when is_binary(ParamKey) -> ok;
    (_) -> error({invalid_template, invalid_param_key})
                    end,
    Rest)
  of
    _ -> ok
  catch
    error:Reason -> {error, Reason}
  end;
check_param_key([_ | _Rest]) ->
  {error, {invalid_template, invalid_param_key}}.

host_to_str(null) -> null;
host_to_str(IPAddr) ->
  list_to_binary(inet:ntoa(IPAddr)).

posix_errno(enoent) -> "The file does not exist";
posix_errno(eacces) ->
  "Missing permission for reading the file, "
  "or for searching one of the parent directorie"
  "s";
posix_errno(eisdir) -> "The named file is a directory";
posix_errno(enotdir) ->
  "A component of the filename is not a "
  "directory";
posix_errno(enomem) ->
  "There is not enough memory for the contents "
  "of the file";
posix_errno(_) -> "Unknown error".

logger_header()() -> "".
