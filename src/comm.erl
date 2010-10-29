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

-export([start_link/2, create_table/0, join_ring/2, broadcast/2]).

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
          members,
          callback,
          pub_count
        }).

-record(member,
        { id,
          pending_ack,
          last_ack_seen
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

start_link(RingName, Callback) ->
    gen_server2:start_link(?MODULE, [RingName, Callback], []).

join_ring(RingName, Callback) ->
    start_link(RingName, Callback).

broadcast(Server, Msg) ->
    gen_server2:cast(Server, {broadcast, Msg}).

init([RingName, Callback]) ->
    gen_server2:cast(self(), join_ring),
    {ok, #state { self       = self(),
                  upstream   = undefined,
                  downstream = undefined,
                  ring_name  = RingName,
                  members    = undefined,
                  callback   = Callback,
                  pub_count  = 0 }, hibernate,
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

handle_cast({activity, Msgs}, State = #state { self       = Self,
                                               downstream = Down,
                                               members    = Members,
                                               callback   = Callback }) ->
    {Members1, Outbound} =
        lists:foldl(
          fun ({Id, Pubs, Acks} = Msg, {Members2, Outbound1}) ->
                  #member { pending_ack = PA, last_ack_seen = LAS } = Member =
                      find_member_or_blank(Id, Members2),
                  {PA1, Outbound2} =
                      case Id of
                          Self ->
                              {PA, [{Id, [], [PubNum || {PubNum, _Pub} <- Pubs]}
                                    | Outbound1]};
                          _ ->
                              {queue:join(PA, queue:from_list(Pubs)),
                               [Msg | Outbound1]}
                      end,
                  {PA2, LAS1} = acks(PA1, Acks, LAS),
                  {dict:store(Id,
                              Member #member { pending_ack   = PA2,
                                               last_ack_seen = LAS1 },
                              Members2),
                   Outbound2}
          end, {Members, []}, Msgs),
    ok = send_downstream(Self, Outbound, Down),
    ok = callback(Callback, Outbound),
    {noreply, State #state { members = Members1 }};

handle_cast({broadcast, Msg}, State = #state { self = Self,
                                               downstream = Down,
                                               members = Members,
                                               pub_count = PubCount }) ->
    PubMsg = {PubCount, Msg},
    Outbound = [{Self, [PubMsg], []}],
    ok = send_downstream(Self, Outbound, Down),
    #member { pending_ack = PA } = Member = find_member_or_blank(Self, Members),
    Members1 = dict:store(Self,
                          Member #member { pending_ack = queue:in(PubMsg, PA) },
                          Members),
    {noreply, State #state { members = Members1, pub_count = PubCount + 1 }};

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
    {noreply, check_neighbours(Ring, State)};

handle_cast({catch_up, Members1}, State = #state { members    = Members,
                                                   downstream = Down,
                                                   self       = Self,
                                                   callback   = Callback }) ->
    {Members2, Outbound} =
        dict:fold(
          fun (Id, #member {}, Acc) when Self =:= Id ->
                  Acc; %% We're certain to know more about ourselves
              (Id, #member { pending_ack = PA1, last_ack_seen = LAS1 },
               {Members3, Outbound1}) ->
                  #member { pending_ack = PA, last_ack_seen = LAS } = Member =
                      find_member_or_blank(Id, Members3),
                  {PA2, Pubs} = subtract_pending_acks(PA1, PA),
                  LAS2 = lists:max([LAS1, LAS]),
                  {PA3, Acks} = catch_up_ack(PA2, LAS2, []),
                  {dict:store(Id,
                              Member #member { pending_ack   = PA3,
                                               last_ack_seen = LAS2 },
                              Members3),
                   [{Id, Pubs, Acks} | Outbound1]}
          end, {Members, []}, Members1),
    ok = send_downstream(Self, Outbound, Down),
    ok = callback(Callback, Outbound),
    {noreply, State #state { members = Members2 }}.

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

find_member_or_blank(Id, Members) ->
    case dict:find(Id, Members) of
        {ok, Result} -> Result;
        error        -> blank_member(Id)
    end.

blank_member(Id) ->
    #member { id            = Id,
              pending_ack   = queue:new(),
              last_ack_seen = -1 }.

subtract_pending_acks(Upstream, Mine) ->
    UpstreamList = queue:to_list(Upstream),
    case queue:out_r(Mine) of
        {empty, _Mine} ->
            {Upstream, UpstreamList};
        {{value, MostRecentPub}, _Mine} ->
            case lists:dropwhile(fun (Pub) -> Pub =/= MostRecentPub end,
                                 UpstreamList) of
                [] ->
                    {Mine, []};
                [MostRecentPub | Pubs] ->
                    {queue:join(Mine, queue:from_list(Pubs)), Pubs}
            end
    end.

acks(Pending, [], LAS) ->
    {Pending, LAS};
acks(Pending, [Ack | Acks], LAS) when Ack =:= LAS + 1 ->
    {{value, {Ack, _Msg}}, Pending1} = queue:out(Pending),
    acks(Pending1, Acks, Ack).

catch_up_ack(Pending, Ack, Acc) ->
    case queue:out(Pending) of
        {{value, {Ack, _Msg}}, Pending1} ->
            {Pending1, lists:reverse(Acc)};
        {{value, {Ack1, _Msg}}, Pending1} when Ack1 < Ack ->
            catch_up_ack(Pending1, Ack, [Ack1 | Acc]);
        {{value, {_Ack1, _Msg}}, _Pending1} ->
            {Pending, Acc};
        {empty, _Pending} when Acc =:= [] ->
            {Pending, Acc}
    end.

send_downstream(Self, _Msg, {Self, undefined}) ->
    ok;
send_downstream(_Self, [], _Down) ->
    ok;
send_downstream(_Self, Msg, {Down, _MRef}) ->
    gen_server2:cast(Down, {activity, Msg}).

callback(Callback, Msgs) ->
    [Callback(Pub) || {_Id, Pubs, _Acks} <- Msgs, {_PubNum, Pub} <- Pubs],
    ok.
