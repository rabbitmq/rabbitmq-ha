%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ-HA.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_ha_sup).

-behaviour(supervisor2).

-export([start_link/0, start_child/1]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(SERVER, ?MODULE).

start_link() ->
    supervisor2:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Args) ->
    application:start(rabbit_ha), %% ensure it's started
    supervisor2:start_child(?SERVER, Args).

init([]) ->
    {ok, {{simple_one_for_one_terminate, 10, 10},
          [{rabbit_ha_queue, {rabbit_ha_queue, start_link, []},
            temporary, ?MAX_WAIT, worker, [rabbit_ha_queue]}]}}.
