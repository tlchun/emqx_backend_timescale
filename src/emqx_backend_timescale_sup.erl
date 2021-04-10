%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午5:04
%%%-------------------------------------------------------------------
-module(emqx_backend_timescale_sup).
-behaviour(supervisor).
-include("../include/emqx_backend_timescale.hrl").

-export([start_link/1]).

-export([init/1]).

start_link(Pools) ->
  supervisor:start_link({local,
    emqx_backend_timescale_sup},
    emqx_backend_timescale_sup,
    [Pools]).

init([Pools]) ->
  {ok, {{one_for_one, 10, 100}, [pool_spec(Pool, Env) || {Pool, Env} <- Pools]}}.

pool_spec(Pool, Env) ->
  ecpool:pool_spec({emqx_backend_timescale, Pool},
    emqx_backend_timescale:pool_name(Pool),
    emqx_backend_timescale_cli,
    Env).
