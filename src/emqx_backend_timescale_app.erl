%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2021 下午5:04
%%%-------------------------------------------------------------------

-module(emqx_backend_timescale_app).
-behaviour(application).

-emqx_plugin(backend).
-include("../include/emqx_backend_timescale.hrl").

-export([start/2, stop/1]).


start(_Type, _Args) ->
  Pools = application:get_env(emqx_backend_timescale,
    pools,
    []),
  {ok, Sup} =
    emqx_backend_timescale_sup:start_link(Pools),
  emqx_backend_timescale:register_metrics(),
  emqx_backend_timescale:load(),
  {ok, Sup}.

stop(_State) -> emqx_backend_timescale:unload().
