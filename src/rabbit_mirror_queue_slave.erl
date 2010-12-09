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

-module(rabbit_mirror_queue_slave).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([joined/2, members_changed/3, handle_msg/3]).

-behaviour(gen_server2).
-behaviour(gm).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("gm_specs.hrl").

-record(state, { q,
                 gm,
                 master_node,
                 backing_queue,
                 backing_queue_state
               }).

start_link(Q) ->
    gen_server2:start_link(?MODULE, [Q], []).

init([#amqqueue { name = QueueName } = Q]) ->
    ok = gm:create_tables(),
    {ok, GM} = gm:start_link(QueueName, ?MODULE, [self()]),
    receive {joined, GM, Nodes} ->
            ok
    end,
    Self = self(),
    Node = node(),
    case rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   [Q1 = #amqqueue { pid = QPid, extra_pids = EPids }] =
                       mnesia:read({rabbit_queue, QueueName}),
                   case [Pid || Pid <- [QPid | EPids], node(Pid) =:= Node] of
                       [] ->
                           EPids1 = EPids ++ [Self],
                           mnesia:write(rabbit_queue,
                                        Q1 #amqqueue { extra_pids = EPids1 },
                                        write),
                           {ok, QPid};
                       _ ->
                           {error, node_already_present}
                   end
           end) of
        {ok, MPid}     -> {ok, BQ} = application:get_env(backing_queue_module),
                          BQS = BQ:init(Q, false),
                          {ok, #state { q           = Q,
                                        gm          = GM,
                                        master_node = node(MPid) }, hibernate,
                           {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
                            ?DESIRED_HIBERNATE}};
        {error, Error} -> {stop, Error}
    end.

handle_call({deliver_immediately, Txn, Message, ChPid}, _From, State) ->
    %% Synchronous, "immediate" delivery mode
    fix_me;

handle_call({deliver, Txn, Message, ChPid}, _From, State) ->
    %% Synchronous, "mandatory" delivery mode
    fix_me;

handle_call({gm_deaths, Deaths}, From,
            State = #state { q           = #amqqueue { name = QueueName },
                             gm          = GM,
                             master_node = MNode }) ->
    io:format("Slave (~p) got deaths: ~p~n", [self(), Deaths]),
    case {node(), node(rabbit_mirror_queue_misc:remove_from_queue(
                         QueueName, Deaths))} of
        {_Node, MNode} ->
            reply(ok, State);
        {Node, Node} ->
            promote_me(From, State);
        {_Node, MNode1} ->
            gen_server2:reply(From, ok),
            ok = gm:broadcast(GM, heartbeat),
            noreply(State #state { master_node = MNode1 })
    end.


handle_cast({deliver, Txn, Message, ChPid}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    fix_me.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, State = #state {}) ->
    %% gen_server case
    ok;
terminate([SPid], Reason) ->
    %% gm case
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined([SPid], Members) ->
    SPid ! {joined, self(), [node(Pid) || Pid <- Members]},
    ok.

members_changed([SPid], Births, Deaths) ->
    io:format("S: ~p ~p ~p~n", [SPid, Births, Deaths]),
    case gen_server2:call(SPid, {gm_deaths, Deaths}) of
        ok              -> ok;
        {promote, CPid} -> {become, rabbit_mirror_queue_coordinator, [CPid]}
    end.

handle_msg([_SPid], _From, heartbeat) ->
    io:format("S: ~p~n", [_SPid]),
    ok;
handle_msg([SPid], From, Msg) ->
    ok.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

promote_me(From, #state { q                   = Q,
                          gm                  = GM,
                          backing_queue       = BQ,
                          backing_queue_state = BQS }) ->
    io:format("Promoting ~p!~n", [self()]),
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(Q, GM),
    true = unlink(GM),
    gen_server2:reply(From, {promote, CPid}),
    ok = gm:broadcast(GM, heartbeat),
    MasterState = rabbit_mirror_queue_master:promote_backing_queue_state(
                    CPid, BQ, BQS),
    QueueState = rabbit_amqqueue_process:init_with_backing_queue_state(
                   Q, rabbit_mirror_queue_master, MasterState),
    {become, rabbit_amqqueue_process, QueueState, hibernate}.
