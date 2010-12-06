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

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([joined/2, members_changed/3, handle_msg/3]).

-behaviour(gen_server2).
-behaviour(gm).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("gm_specs.hrl").

-record(state, { name,
                 gm
               }).

-define(ONE_SECOND, 1000).

start_link(QueueName) ->
    gen_server2:start_link(?MODULE, [QueueName], []).

add_slave(CPid, SlaveNode) ->
    gen_server2:cast(CPid, {add_slave, SlaveNode}).

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init([QueueName]) ->
    ok = gm:create_tables(),
    {ok, GM} = gm:start_link(QueueName, ?MODULE, [self()]),
    receive {joined, GM, _Members} ->
            ok
    end,
    {ok, _TRef} =
        timer:apply_interval(?ONE_SECOND, gm, broadcast, [GM, heartbeat]),
    {ok, #state { name = QueueName, gm = GM }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({add_slave, Node}, State = #state { name = QueueName }) ->
    Result = rabbit_mirror_queue_slave_sup:start_child(Node, [QueueName]),
    io:format("Add Slave '~p' => ~p~n", [Node, Result]),
    noreply(State);

handle_cast({gm_deaths, Deaths}, State = #state { gm   = GM,
                                                  name = QueueName }) ->
    io:format("Coord (~p) got deaths: ~p~n", [self(), Deaths]),
    Node = node(),
    Node = node(rabbit_mirror_queue_misc:remove_from_queue(QueueName, Deaths)),
    noreply(State).

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, State = #state{}) ->
    %% gen_server case
    ok;
terminate([CPid], Reason) ->
    %% gm case
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined([CPid], Members) ->
    CPid ! {joined, self(), Members},
    ok.

members_changed([CPid], Births, Deaths) ->
    io:format("C: ~p ~p ~p~n", [CPid, Births, Deaths]),
    ok = gen_server2:cast(CPid, {gm_deaths, Deaths}).

handle_msg([_CPid], _From, heartbeat) ->
    ok;
handle_msg([CPid], From, Msg) ->
    ok.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.
