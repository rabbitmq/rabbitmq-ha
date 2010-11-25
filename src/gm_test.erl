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

-module(gm_test).

-export([test/0]).
-export([joined/1, members_changed/2, handle_msg/2, terminate/1]).

-behaviour(gm).

get_state() ->
    get(?MODULE).

with_state(Fun) ->
    put(?MODULE, Fun(get_state())).

joined([Members]) ->
    io:format("Joined ~p (~p members)~n", [self(), length(Members)]),
    put(?MODULE, dict:new()),
    ok.

members_changed(Births, Deaths) ->
    with_state(
      fun (State) ->
              State1 =
                  lists:foldl(
                    fun (Born, StateN) ->
                            false = dict:is_key(Born, StateN),
                            dict:store(Born, empty, StateN)
                    end, State, Births),
              lists:foldl(
                fun (Died, StateN) ->
                        dict:erase(Died, StateN)
                end, State1, Deaths)
      end),
    ok.

handle_msg(From, {test_msg, Num}) ->
    with_state(
      fun (State) ->
              ok = case dict:find(From, State) of
                       {ok, empty} -> ok;
                       {ok, Num}   -> ok;
                       error       -> ok;
                       {ok, Num1} when Num1 < Num ->
                           exit({{duplicate_delivery_of, Num1},
                                 {expecting, Num}});
                       {ok, Num1} ->
                           exit({{missing_delivery_of, Num},
                                 {received_early, Num1}})
                   end,
              dict:store(From, Num + 1, State)
      end),
    ok.

terminate(Reason) ->
    io:format("Left ~p (~p)~n", [self(), Reason]),
    ok.

spawn_member() ->
    spawn_link(
      fun () ->
              random:seed(now()),
              %% start up delay of no more than 10 seconds
              timer:sleep(random:uniform(10000)),
              {ok, Pid} = gm:start_link(?MODULE, ?MODULE, []),
              Start = random:uniform(10000),
              send_loop(Pid, Start, Start + random:uniform(10000)),
              gm:leave(Pid),
              spawn_more()
      end).

spawn_more() ->
    [spawn_member() || _ <- lists:seq(1, 4 - random:uniform(4))].

send_loop(_Pid, Target, Target) ->
    ok;
send_loop(Pid, Count, Target) when Target > Count ->
    gm:broadcast(Pid, {test_msg, Count}),
    timer:sleep(random:uniform(5) - 1), %% sleep up to 4 ms
    send_loop(Pid, Count + 1, Target).

test() ->
    ok = gm:create_tables(),
    spawn_member(),
    spawn_member().
