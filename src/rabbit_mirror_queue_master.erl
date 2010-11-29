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

-export([init/3, terminate/1, delete_and_terminate/1,
         purge/1, publish/3, publish_delivered/4, fetch/2, ack/2,
         tx_publish/4, tx_ack/3, tx_rollback/2, tx_commit/4,
         requeue/3, len/1, is_empty/1, dropwhile/2,
         set_ram_duration_target/2, ram_duration/1,
         needs_idle_timeout/1, idle_timeout/1, handle_pre_hibernate/1,
         status/1]).

-export([start/1, stop/0]).

-export([joined/1, members_changed/2, handle_msg/2]).

-behaviour(rabbit_backing_queue).
-behaviour(gm).

-record(state, { gm,
                 vq }).

%% ---------------------------------------------------------------------------
%% Backing queue
%% ---------------------------------------------------------------------------

start(DurableQueues) ->
    ok.

stop() ->
    ok.

init(QueueName, IsDurable, Recover) ->
    #state {}.

terminate(#state {}) ->
    %% backing queue termination
    ok;
terminate(Reason) ->
    %% gm termination
    ok.

delete_and_terminate(#state {} = State) ->
    State.

purge(#state {} = State) ->
    {0, State}.

publish(Msg, MsgProps, #state {} = State) ->
    State.

publish_delivered(AckRequired, Msg, MsgProps, #state {} = State) ->
    {blank_ack, State}.

dropwhile(Fun, #state {} = State) ->
    State.

fetch(AckRequired, #state {} = State) ->
    {empty, State}.

ack(AckTags, #state {} = State) ->
    State.

tx_publish(Txn, Msg, MsgProps, #state {} = State) ->
    State.

tx_ack(Txn, AckTags, #state {} = State) ->
    State.

tx_rollback(Txn, #state {} = State) ->
    State.

tx_commit(Txn, PostCommitFun, MsgPropsFun, #state {} = State) ->
    {[], State}.

requeue(AckTags, MsgPropsFun, #state {} = State) ->
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

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined(Members) ->
    ok.

members_changed(Births, Deaths) ->
    ok.

handle_msg(From, Msg) ->
    ok.
