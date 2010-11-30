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

-export([joined/1, members_changed/1, handle_msg/1, terminate/1]).

-behaviour(gen_server2).
-behaviour(gm).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("gm_specs.hrl").

-record(state, { name,
                 gm,
                 master_pid,
                 master_in_gm,
                 promote_on_gm_death_of
               }).

start_link(QueueName) ->
    gen_server2:start_link(?MODULE, [QueueName], []).

init([QueueName]) ->
    ok = gm:create_tables(),
    {ok, GM} = gm:start_link(QueueName, ?MODULE, [self()]),
    receive {joined, GM, Members} ->
            ok
    end,
    Self = self(),
    MPid = rabbit_misc:execute_mnesia_transaction(
             fun () ->
                     [Q = #amqqueue { pid = QPid, extra_pids = EPids }] =
                         mnesia:read({rabbit_queue, QueueName}),
                     EPids1 = EPids ++ [Self],
                     mnesia:write(rabbit_queue,
                                  Q #amqqueue { extra_pids = EPids1 },
                                  write),
                     QPid
             end),
    erlang:monitor(process, MPid),
    {ok, #state { name                   = QueueName,
                  gm                     = GM,
                  master_pid             = MPid,
                  master_in_gm           = lists:member(MPid, Members),
                  promote_on_gm_death_of = undefined }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({deliver_immediately, Txn, Message, ChPid}, _From, State) ->
    %% Synchronous, "immediate" delivery mode
    fix_me;

handle_call({deliver, Txn, Message, ChPid}, _From, State) ->
    %% Synchronous, "mandatory" delivery mode
    fix_me.

handle_cast({deliver, Txn, Message, ChPid}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    fix_me;

handle_cast({gm_deaths, Deaths},
            State = #state { master_pid = MPid,
                             master_in_gm = MInGM,
                             promote_on_gm_death_of = OldMPid }) ->
    MNode = node(MPid),
    MDied = lists:any(fun (Pid) -> node(Pid) =:= MNode end, Deaths),
    MInGM1 = MInGM andalso not MDied,
    State1 = State #state { master_in_gm = MInGM1 },
    noreply(
      case OldMPid of
          undefined ->
              State1;
          _ ->
              OldMNode = node(OldMPid),
              OldMDied = lists:any(fun (Pid) -> node(Pid) =:= OldMNode end,
                                   Deaths),
              promote_me(OldMPid, State1)
      end).

handle_info({'DOWN', _MRef, process, MPid, _Reason},
            State = #state { name       = QueueName,
                             master_pid = MPid }) ->
    MPid1 = remove_from_queue(QueueName, MPid),
    State1 = State #state { master_pid = MPid1 },
    noreply(case self() of
                MPid1 -> promote_me(MPid, State1);
                _     -> State1
            end).

terminate(_Reason, State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined(#gm_joined { args = [SPid], members = Members }) ->
    SPid ! {joined, self(), Members},
    ok.

members_changed(#gm_members_changed {
                   args = [SPid], births = Births, deaths = Deaths }) ->
    gen_server2:cast(SPid, {gm_deaths, Deaths}),
    ok.

handle_msg(#gm_handle_msg { args = [SPid], from = From, msg = Msg }) ->
    ok.

terminate(#gm_terminate { args = [SPid], reason = Reason }) ->
    ok.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

remove_from_queue(QueueName, MPid) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              [Q = #amqqueue { pid        = Pid,
                               extra_pids = [EPid | EPids] }] =
                  mnesia:read({rabbit_queue, QueueName}),
              case Pid of
                  MPid -> Q1 = Q #amqqueue { pid        = EPid,
                                             extra_pids = EPids },
                          mnesia:write(rabbit_queue, Q1, write),
                          EPid;
                  _    -> Pid
              end
      end).

promote_me(OldMPid, State = #state { master_in_gm = true,
                                     promote_on_gm_death_of = undefined }) ->
    State #state { promote_on_gm_death_of = OldMPid };
promote_me(OldMPid, State = #state { master_in_gm = false }) ->
    todo.
