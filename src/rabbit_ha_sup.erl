-module(rabbit_ha_sup).

-behaviour(supervisor2).

-export([start_link/0, start_child/1]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(SERVER, ?MODULE).

start_link() ->
    supervisor2:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Args) ->
    supervisor2:start_child(?SERVER, Args).

init([]) ->
    {ok, {{simple_one_for_one_terminate, 10, 10},
          [{rabbit_ha_queue, {rabbit_ha_queue, start_link, []},
            temporary, ?MAX_WAIT, worker, [rabbit_ha_queue]}]}}.
