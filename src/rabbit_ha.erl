-module(rabbit_ha).

-export([start/2, stop/1]).

start(normal, []) -> rabbit_ha_sup:start_link().

stop(_State)      -> ok.
