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

%% We join the GM group before we add ourselves to the amqqueue
%% record. As a result:
%% 1. We can receive msgs from GM that correspond to messages we will
%% never receive from publishers.
%% 2. When we receive a message from publishers, we must receive a
%% message from the GM group for it.
%% 3. However, that instruction from the GM group can arrive either
%% before or after the actual message. We need to be able to
%% distinguish between GM instructions arriving early, and case (1)
%% above.
%%
%% Thus, per sender, we need two queues: one to hold messages from
%% publishers, and one to hold instructions from the GM group. It's
%% always the case that only one of these queues will ever be
%% non-empty (thus the implementation might actually get away with
%% just one queue).
%%
%% On receipt of a GM group instruction, three things are possible:
%% 1. The queue of publisher messages is empty. Thus store the GM
%% instruction to the relevant queue.
%% 2. The head of the queue of publisher messages has a message that
%% matches the GUID of the GM instruction. Remove the message, and
%% route appropriately.
%% 3. The head of the queue of publisher messages has a message that
%% does not match the GUID of the GM instruction. Throw away the GM
%% instruction.
%%
%% On receipt of a publisher message, three things are possible:
%% 1. The queue of GM group instructions is empty. Add the message to
%% the relevant queue and await instructions from the GM.
%% 2. The head of the queue of GM group instructions has an
%% instruction matching the GUID of the message. Remove that
%% instruction and act on it.
%% 3. The head of the queue of GM group instructions has an
%% instruction that does not match the GUID of the message. Throw away
%% the GM group instruction and repeat - attempt to match against the
%% next instruction if there is one.
%%
%% In all cases, we are relying heavily on order preserving messaging
%% both from the GM group and from the publishers.

-export([start_link/1, set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1]).

-export([joined/2, members_changed/3, handle_msg/3]).

-behaviour(gen_server2).
-behaviour(gm).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("gm_specs.hrl").

-record(state, { q,
                 gm,
                 master_node,
                 backing_queue,
                 backing_queue_state,
                 rate_timer_ref
               }).

-define(RAM_DURATION_UPDATE_INTERVAL,  5000).

start_link(Q) ->
    gen_server2:start_link(?MODULE, [Q], []).

set_maximum_since_use(QPid, Age) ->
    gen_server2:cast(QPid, {set_maximum_since_use, Age}).

init([#amqqueue { name = QueueName } = Q]) ->
    process_flag(trap_exit, true), %% amqqueue_process traps exits too.
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
        {ok, MPid} ->
            ok = file_handle_cache:register_callback(
                   rabbit_amqqueue, set_maximum_since_use, [self()]),
            ok = rabbit_memory_monitor:register(
                   self(), {rabbit_amqqueue, set_ram_duration_target,
                            [self()]}),
            {ok, BQ} = application:get_env(backing_queue_module),
            BQS = BQ:init(Q, false),
            {ok, #state { q                   = Q,
                          gm                  = GM,
                          master_node         = node(MPid),
                          backing_queue       = BQ,
                          backing_queue_state = BQS,
                          rate_timer_ref      = undefined }, hibernate,
             {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
              ?DESIRED_HIBERNATE}};
        {error, Error} ->
            {stop, Error}
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
    fix_me;

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State);

handle_cast({set_ram_duration_target, Duration},
            State = #state { backing_queue       = BQ,
                             backing_queue_state = BQS }) ->
    BQS1 = BQ:set_ram_duration_target(Duration, BQS),
    noreply(State #state { backing_queue_state = BQS1 }).

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, State = #state {}) ->
    %% gen_server case
    %% TODO: figure out what to do with the backing queue. See
    %% amqqueue_process:terminate/2
    ok;
terminate([SPid], Reason) ->
    %% gm case
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_pre_hibernate(State = #state { backing_queue       = BQ,
                                      backing_queue_state = BQS }) ->
    %% mainly copied from amqqueue_process
    BQS1 = BQ:handle_pre_hibernate(BQS),
    %% no activity for a while == 0 egress and ingress rates
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), infinity),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    {hibernate, stop_rate_timer(State #state { backing_queue_state = BQS2 })}.

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

promote_me(From, #state { q                   = Q,
                          gm                  = GM,
                          backing_queue       = BQ,
                          backing_queue_state = BQS,
                          rate_timer_ref      = RateTRef }) ->
    io:format("Promoting ~p!~n", [self()]),
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(Q, GM),
    true = unlink(GM),
    gen_server2:reply(From, {promote, CPid}),
    ok = gm:broadcast(GM, heartbeat),
    MasterState = rabbit_mirror_queue_master:promote_backing_queue_state(
                    CPid, BQ, BQS),
    QueueState = rabbit_amqqueue_process:init_with_backing_queue_state(
                   Q, rabbit_mirror_queue_master, MasterState, RateTRef),
    {become, rabbit_amqqueue_process, QueueState, hibernate}.

noreply(State) ->
    {noreply, next_state(State), hibernate}.

reply(Reply, State) ->
    {reply, Reply, next_state(State), hibernate}.

next_state(State) ->
    ensure_rate_timer(State).

%% copied+pasted from amqqueue_process
ensure_rate_timer(State = #state { rate_timer_ref = undefined }) ->
    {ok, TRef} = timer:apply_after(
                   ?RAM_DURATION_UPDATE_INTERVAL,
                   rabbit_amqqueue, update_ram_duration,
                   [self()]),
    State #state { rate_timer_ref = TRef };
ensure_rate_timer(State = #state { rate_timer_ref = just_measured }) ->
    State #state { rate_timer_ref = undefined };
ensure_rate_timer(State) ->
    State.

stop_rate_timer(State = #state { rate_timer_ref = undefined }) ->
    State;
stop_rate_timer(State = #state { rate_timer_ref = just_measured }) ->
    State #state { rate_timer_ref = undefined };
stop_rate_timer(State = #state { rate_timer_ref = TRef }) ->
    {ok, cancel} = timer:cancel(TRef),
    State #state { rate_timer_ref = undefined }.
