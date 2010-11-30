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

-module(rabbit_mirror_queue_master).

-export([init/2, terminate/1, delete_and_terminate/1,
         purge/1, publish/4, publish_delivered/5, fetch/2, ack/2,
         tx_publish/5, tx_ack/3, tx_rollback/2, tx_commit/4,
         requeue/3, len/1, is_empty/1, dropwhile/2,
         set_ram_duration_target/2, ram_duration/1,
         needs_idle_timeout/1, idle_timeout/1, handle_pre_hibernate/1,
         status/1]).

-export([start/1, stop/0]).

-behaviour(rabbit_backing_queue).

-include_lib("rabbit_common/include/rabbit.hrl").

-record(state, { coordinator, vq }).

%% ---------------------------------------------------------------------------
%% Backing queue
%% ---------------------------------------------------------------------------

start(_DurableQueues) ->
    %% This will never get called as this module will never be
    %% installed as the default BQ implementation.
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

stop() ->
    %% Same as start/1.
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

init(#amqqueue { name = QueueName, arguments = Args, durable = false },
     Recover) ->
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(QueueName),
    {_Type, Nodes} = rabbit_misc:table_lookup(Args, <<"x-mirror">>),
    [rabbit_mirror_queue_coordinator:add_slave(CPid, binary_to_atom(Node, utf8))
     || {longstr, Node} <- Nodes],
    #state { coordinator = CPid }.

terminate(#state {}) ->
    %% backing queue termination
    ok.

delete_and_terminate(#state {} = State) ->
    State.

purge(#state {} = State) ->
    %% get count and gm:broadcast(GM, {drop_next, Count})
    {0, State}.

publish(Msg, MsgProps, ChPid, #state {} = State) ->
    %% gm:broadcast(GM, {publish, Guid, MsgProps, ChPid})
    State.

publish_delivered(AckRequired, Msg, MsgProps, ChPid, #state {} = State) ->
    %% gm:broadcast(GM, {publish_delivered, AckRequired, Guid, MsgProps, ChPid})
    %% prefix acktag with self()
    {blank_ack, State}.

dropwhile(Fun, #state {} = State) ->
    %% DropCount = len(State) - len(State1),
    %% gm:broadcast(GM, {drop_next, DropCount})
    State.

fetch(AckRequired, #state {} = State) ->
    %% gm:broadcast(GM, {fetch, Guid})
    %% prefix acktag with self()
    {empty, State}.

ack(AckTags, #state {} = State) ->
    %% gm:broadcast(GM, {ack, Guids})
    %% drop any acktags which do not have self() prefix
    State.

tx_publish(Txn, Msg, MsgProps, ChPid, #state {} = State) ->
    %% gm:broadcast(GM, {tx_publish, Txn, Guid, MsgProps, ChPid})
    State.

tx_ack(Txn, AckTags, #state {} = State) ->
    %% gm:broadcast(GM, {tx_ack, Txn, Guids})
    State.

tx_rollback(Txn, #state {} = State) ->
    %% gm:broadcast(GM, {tx_rollback, Txn})
    State.

tx_commit(Txn, PostCommitFun, MsgPropsFun, #state {} = State) ->
    %% Maybe don't want to transmit the MsgPropsFun but what choice do
    %% we have? OTOH, on the slaves, things won't be expiring on their
    %% own (props are interpreted by amqqueue, not vq), so if the msg
    %% props aren't quite the same, that doesn't matter.
    %%
    %% The PostCommitFun is actually worse - we need to prevent that
    %% from being invoked until we have confirmation from all the
    %% slaves that they've done everything up to there.
    %%
    %% In fact, transactions are going to need work seeing as it's at
    %% this point that VQ mentions amqqueue, which will thus not work
    %% on the slaves - we need to make sure that all the slaves do the
    %% tx_commit_post_msg_store at the same point, and then when they
    %% all confirm that (scatter/gather), we can finally invoke the
    %% PostCommitFun.
    %%
    %% Another idea is that the slaves are actually driven with
    %% pubacks and thus only the master needs to support txns
    %% directly.
    {[], State}.

requeue(AckTags, MsgPropsFun, #state {} = State) ->
    %% gm:broadcast(GM, {requeue, Guids}),
    State.

len(#state {}) ->
    0.

is_empty(State) ->
    0 =:= len(State).

set_ram_duration_target(Target, #state {} = State) ->
    State.

ram_duration(#state {} = State) ->
    {0, State}.

needs_idle_timeout(#state {} = State) ->
    false.

idle_timeout(#state {} = State) ->
    State.

handle_pre_hibernate(#state {} = State) ->
    State.

status(#state {}) ->
    [].
