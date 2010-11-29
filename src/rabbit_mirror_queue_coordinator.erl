%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_coordinator).

-export([start_link/1, add_slave/2]).

-export([joined/1, members_changed/2, handle_msg/2, terminate/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-behaviour(gm).
-behaviour(gen_server2).

-include_lib("rabbit_common/include/rabbit.hrl").

-record(state, { name,
                 gm,
                 joined
               }).

start_link(QueueName) ->
    gen_server2:start_link(?MODULE, [QueueName], []).

add_slave(CPid, SlaveNode) ->
    gen_server2:cast(CPid, {add_slave, SlaveNode}).

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init([QueueName, Args]) ->
    ok = gm:create_tables(),
    {ok, GM} = gm:start_link(QueueName, ?MODULE, [self()]),
    receive {joined, GM, _Members} ->
            ok
    end,
    {ok, #state { name   = QueueName,
                  gm     = GM,
                  joined = false }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({joined, _Members}, State = #state { joined = false }) ->
    noreply(State #state { joined = true });

handle_cast({add_slave, SlaveNode}, State = #state { name   = QueueName,
                                                     joined = true }) ->
    {ok, SPid} = rabbit_mirror_queue_slave:start_link(SlaveNode, QueueName),
    noreply(State).

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined([CPid, Members]) ->
    CPid ! {joined, self(), Members},
    ok.

members_changed(Births, Deaths) ->
    ok.

handle_msg(From, Msg) ->
    ok.

terminate(Reason) ->
    ok.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.
