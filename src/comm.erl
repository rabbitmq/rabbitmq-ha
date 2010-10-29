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

-module(comm).

-behaviour(gen_server2).

-export([start_link/1, create_table/0, join_ring/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TABLE, ring).
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(state,
        { self,
          upstream,
          downstream,
          ring_name,
          members
        }).

-record(member,
        { id,
          pending_ack,
          last_ack_seen,
          pub_count
        }).

-record(ring, { name, members }).

create_table() ->
    case mnesia:create_table(
           ?TABLE, [{record_name, ring},
                    {attributes, record_info(fields, ring)}]) of
        {atomic, ok}                    -> ok;
        {aborted,{already_exists,ring}} -> ok;
        Err                             -> Err
    end.

join_ring(RingName) ->
    start_link(RingName).

start_link(RingName) ->
    gen_server2:start_link(?MODULE, [RingName], []).

init([RingName]) ->
    gen_server2:cast(self(), join_ring),
    {ok, #state { self       = self(),
                  upstream   = undefined,
                  downstream = undefined,
                  ring_name  = RingName,
                  members    = undefined }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({add_new_member, Member}, _From,
            State = #state { self      = Self,
                             ring_name = RingName,
                             members   = Members }) ->
    {atomic, Result} =
        mnesia:sync_transaction(
          fun () ->
                  [Ring = #ring { members = Members1 }] =
                      mnesia:read(?TABLE, RingName),
                  Q = queue:from_list(Members1),
                  case queue:out_r(Q) of
                      {{value, Self}, _Q1} ->
                          Q1 = queue:to_list(queue:in(Member, Q)),
                          Ring1 = Ring #ring { members = Q1 },
                          ok = mnesia:write(Ring1),
                          {ok, Ring1};
                      {{value, _Member}, _Q1} ->
                          not_most_recently_added
                  end
          end),
    case Result of
        {ok, Ring2} ->
            {reply, {ok, Ring2, Members}, check_neighbours(Ring2, State)};
        not_most_recently_added ->
            {reply, not_most_recently_added, State}
    end.

handle_cast(join_ring, State = #state { self       = Self,
                                        upstream   = undefined,
                                        downstream = undefined,
                                        ring_name  = RingName,
                                        members    = undefined }) ->
    {Ring, Members} = case maybe_create_ring(RingName, Self) of
                          {new, Ring1}      -> {Ring1, dict:new()};
                          {existing, Ring1} -> internal_join_ring(Self, Ring1)
                      end,
    {Up, Down} = find_neighbours(Ring, Self),
    {noreply, State #state { upstream   = {Up, maybe_monitor(Up, Self)},
                             downstream = {Down, maybe_monitor(Down, Self)},
                             members    = Members }};

handle_cast(check_neighbours, State = #state { ring_name  = RingName }) ->
    {ok, Ring} = read_ring(RingName),
    {noreply, check_neighbours(Ring, State)}.

handle_info({'DOWN', MRef, process, _Pid, _Reason},
            State = #state { self       = Self,
                             upstream   = Up,
                             downstream = Down,
                             ring_name  = RingName,
                             members    = Members }) ->
    Member = case {Up, Down} of
                 {{Member1, MRef}, _} -> Member1;
                 {_, {Member1, MRef}} -> Member1;
                 _                    -> unknown
             end,
    Ring =
        case Member of
            unknown ->
                {ok, Ring1} = read_ring(RingName),
                Ring1;
            _ ->
                {atomic, Ring1} =
                    mnesia:sync_transaction(
                      fun () ->
                              [Ring2 = #ring { members = Members1 }] =
                                  mnesia:read(?TABLE, RingName),
                              case lists:delete(Member, Members1) of
                                  Members1 -> Ring2;
                                  Members2 -> Ring3 = Ring2 #ring {
                                                        members = Members2 },
                                              mnesia:write(Ring3),
                                              Ring3
                              end
                      end),
                Ring1
        end,
    State1 = #state { downstream = Down1 } = check_neighbours(Ring, State),
    ok = case Down1 of
             Down               -> ok;
             {Self, undefined}  -> ok;
             {Neighbour, _MRef} -> gen_server2:cast(Neighbour,
                                                    {catch_up, Members})
         end,
    {noreply, State1}.

terminate(_Reason, _State) ->
    io:format("~p death ~p ~p~n", [self(), _Reason, _State]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

find_neighbours(#ring { members = Members }, Self) ->
    find_neighbours1(queue:from_list(Members), Self).

find_neighbours1(Q, Self) ->
    case queue:out(Q) of
        {{value, Self}, Q1} ->
            {{value, Up}, _Q2} = queue:out_r(Q),
            {{value, Down}, _Q3} = queue:out(queue:in(Self, Q1)),
            {Up, Down};
        {{value, Other}, Q1} ->
            find_neighbours1(queue:in(Other, Q1), Self)
    end.

read_ring(RingName) ->
    case mnesia:dirty_read(?TABLE, RingName) of
        []     -> {error, not_found};
        [Ring] -> {ok, Ring}
    end.

maybe_create_ring(RingName, Self) ->
    case read_ring(RingName) of
        {error, not_found} ->
            {atomic, Result} =
                mnesia:sync_transaction(
                  fun () ->
                          case mnesia:read(?TABLE, RingName) of
                              [] ->
                                  Ring = #ring{ name = RingName,
                                                members = [Self] },
                                  ok = mnesia:write(Ring),
                                  {new, Ring};
                              [Ring] ->
                                  {existing, Ring}
                          end
                  end),
            Result;
        {ok, Ring} ->
            {existing, Ring}
    end.

maybe_monitor(Self, Self) ->
    undefined;
maybe_monitor(Other, _Self) ->
    erlang:monitor(process, Other).

ensure_neighbour(Self, {Self, undefined}, Self) ->
    {Self, undefined};
ensure_neighbour(Self, {Self, undefined}, RealNeighbour) ->
    ok = gen_server2:cast(RealNeighbour, check_neighbours),
    {RealNeighbour, maybe_monitor(RealNeighbour, Self)};
ensure_neighbour(_Self, {RealNeighbour, MRef}, RealNeighbour) ->
    {RealNeighbour, MRef};
ensure_neighbour(Self, {RealNeighbour, MRef}, Neighbour) ->
    true = erlang:demonitor(MRef),
    ok = gen_server2:cast(RealNeighbour, check_neighbours),
    case Neighbour of
        Self -> {Self, undefined};
        _    -> ok = gen_server2:cast(Neighbour, check_neighbours),
                {Neighbour, maybe_monitor(Neighbour, Self)}
    end.

internal_join_ring(Self, #ring { name = RingName, members = Members }) ->
    [MostRecentlyAdded | _] = lists:reverse(Members),
    Result = gen_server2:call(
               MostRecentlyAdded, {add_new_member, Self}, infinity),
    case Result of
        {ok, Ring, Members1}     -> {Ring, Members1};
        not_most_recently_added  -> {ok, Ring} = read_ring(RingName),
                                    internal_join_ring(Self, Ring)
    end.

check_neighbours(Ring, State = #state { self       = Self,
                                        upstream   = Up,
                                        downstream = Down }) ->
    {Up1, Down1} = find_neighbours(Ring, Self),
    Up2 = ensure_neighbour(Self, Up, Up1),
    Down2 = ensure_neighbour(Self, Down, Down1),
    State #state { upstream = Up2, downstream = Down2 }.
