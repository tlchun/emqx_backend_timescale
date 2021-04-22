%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午5:04
%%%-------------------------------------------------------------------
-module(emqx_backend_timescale).

-export([logger_header/0]).
-include("../include/emqx_backend_timescale.hrl").
-include("../include/emqx.hrl").


-export([pool_name/1]).

-export([register_metrics/0, load/0, unload/0]).

-export([on_message_publish/2]).


pool_name(Pool) -> list_to_atom(lists:concat([emqx_backend_timescale, '_', Pool])).

register_metrics() ->
  [emqx_metrics:new(MetricName) || MetricName <- ['backend.timescale.message_publish']].

load() ->
  HookList = parse_hook(application:get_env(emqx_backend_timescale, hooks, [])),
  Templates = emqx_backend_timescale_cli:get_templates(),
  lists:foreach(fun ({Hook, Action, Pool, Filter}) ->
    load_(Hook, b2a(proplists:get_value(<<"function">>, Action)), {Filter, Pool, Templates}) end, HookList),
  io:format("~s is loaded.~n", [emqx_backend_timescale]),
  ok.

load_(Hook, Fun, Params) ->
  case Hook of
    'message.publish' -> emqx:hook(Hook, fun emqx_backend_timescale:Fun/2, [Params])
  end.

unload() ->
  HookList = parse_hook(application:get_env(emqx_backend_timescale, hooks, [])),
  lists:foreach(fun ({Hook, Action, _Pool, _Filter}) ->
    case proplists:get_value(<<"function">>, Action) of
      undefined -> ok;
      Fun -> unload_(Hook, b2a(Fun))
    end
                end,
    HookList),
  io:format("~s is unloaded.~n", [emqx_backend_timescale]),
  ok.

unload_(Hook, Fun) ->
  case Hook of
    'message.publish' -> emqx:unhook(Hook, fun emqx_backend_timescale:Fun/2)
  end.

on_message_publish(Message = #message{flags = #{retain := true}, payload = <<>>}, _Params) ->
  {ok, Message};
on_message_publish(Message = #message{topic = Topic}, {Filter, Pool, Templates}) ->
  with_filter(fun () ->
    try emqx_backend_timescale_cli:prepare_data(Message, Templates) of
      Data ->
        emqx_metrics:inc('backend.timescale.message_publish'),
        emqx_backend_timescale_cli:pgsql_insert(Pool, Data),
        begin
          logger:log(debug, #{}, #{report_cb => fun (_) -> {logger_header() ++ "Write point ~p to Timescale successfully", [Data]} end, mfa => {emqx_backend_timescale, on_message_publish, 2}, line => 79})
        end
    catch
      error:no_available_template:_Stacktrace ->
        begin
          logger:log(debug,
            #{},
            #{report_cb =>
            fun (_) ->
              {logger_header()
                ++
                "Build Timescale point failed: no_available_te"
                "mplate",
                []}
            end,
              mfa =>
              {emqx_backend_timescale,
                on_message_publish,
                2},
              line => 82})
        end;
      error:Reason:Stacktrace ->
        begin
          logger:log(error,
            #{},
            #{report_cb =>
            fun (_) ->
              {logger_header()
                ++
                "Build Timescale point failed: ~p, ~p",
                [Reason,
                  Stacktrace]}
            end,
              mfa =>
              {emqx_backend_timescale,
                on_message_publish,
                2},
              line => 84})
        end
    end,
    {ok, Message}
              end,
    Message,
    Topic,
    Filter).

parse_hook(Hooks) -> parse_hook(Hooks, []).

parse_hook([], Acc) -> Acc;
parse_hook([{Hook, Item} | Hooks], Acc) ->
  Params = emqx_json:decode(Item),
  Action = proplists:get_value(<<"action">>, Params),
  Pool = proplists:get_value(<<"pool">>, Params),
  Filter = proplists:get_value(<<"topic">>, Params),
  parse_hook(Hooks, [{l2a(Hook), Action, pool_name(b2a(Pool)), Filter} | Acc]).

with_filter(Fun, _, _, undefined) -> Fun();
with_filter(Fun, Msg, Topic, Filter) ->
  case emqx_topic:match(Topic, Filter) of
    true -> Fun();
    false -> {ok, Msg}
  end.

l2a(L) -> erlang:list_to_atom(L).

b2a(B) -> erlang:binary_to_atom(B, utf8).

logger_header() -> "".
