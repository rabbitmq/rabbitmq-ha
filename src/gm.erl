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

-module(gm).

%% Guaranteed Multicast
%%
%% This module provides the ability to create named groups of
%% processes to which members can be dynamically added and removed,
%% and for messages to be broadcast within the group that are
%% guaranteed to reach all members of the group during the lifetime of
%% the message. The lifetime of a message is defined as being, at a
%% minimum, the time from which the message is first sent to any
%% member of the group, up until the time at which it is known by the
%% member who created the message that the message has reached all
%% group members.
%%
%% The guarantee given is that provided a message, once sent, makes it
%% to members who do not all leave the group, the message will
%% continue to propogate to all group members, including new group
%% members, provided the new members have joined before the message
%% has reached all members of the group.
%%
%% One possible means of implementation would be a fanout from the
%% sender to every member of the group. This would require that the
%% group is fully connected, and, in the event that the original
%% sender of the message disappears from the group before the message
%% has made it to every member of the group, raises questions as to
%% who is responsible for sending on the message to new group members.
%% In the event that within the group, messages sent are broadcast
%% from a subset of the members, this arrangement has the potential to
%% substantially impact the CPU and network workload of such members,
%% as such members would have to accomodate the cost of sending each
%% message to every group member.
%%
%% Instead, if the members of the group are arranged in a chain, then
%% it makes it much easier to ensure that it is possible to reason
%% about who within the group has received the message and who has
%% not. It eases issues of responsibility: in the event of a group
%% member disappearing, the nearest upstream member of the chain is
%% responsible for ensuring that messages continue to propogate down
%% the chain. It also results in equal distribution of sending and
%% receiving workload, even if all messages are being sent from just a
%% single group member. This configuration has the further advantage
%% that it is not necessary for every group member to know of every
%% other group member, and even that a group member does not have to
%% be accessible from all other group members.
%%
%% Performance is kept high by permitting pipelining and all
%% communication is asynchronous. In the chain A -> B -> C, if A sends
%% a message to the group, it will not directly contact C. However, it
%% must know that C receives the message (in addition to B) before it
%% can consider the message fully sent. A simplistic implementation
%% would require that C replies to B and B then replies to A. This
%% would then mean that the propagation delay is twice the length of
%% the chain. It would also require, in the event of the failure of B,
%% that C knows to directly contact A and issue the necessary
%% replies. Instead, the chain forms a ring: C sends the message on to
%% A: C does not distinguish A as the sender, merely as the next
%% member within the chain. When A receives from C messages that A
%% sent, it knows that all participants have received the
%% message. However, the message is not dead yet: if C died as B was
%% sending to C, then B would need to detect the death of C and
%% forward the message on to A instead: thus every node has to
%% remember every message published to it until it is told that it can
%% forget about the message. This is essential not just for dealing
%% with failure of members, but also for the addition of new members.
%%
%% Thus once A receives the message back again, it then sends to B an
%% acknowledgement for the message, indicating that B can now forget
%% about the message. B does so, and forwards the ack to C. C forgets
%% the message, and forwards the ack back to A. At this point, A takes
%% no further action: the message and its acknowledgement have made it
%% to every member of the group. The message is now dead, and any new
%% member joining the group at this point will not receive the
%% message.
%%
%% We therefore have two roles:
%%
%% 1. The sender, who upon receiving their own messages back, must
%% then send out acknowledgements, and upon receiving their own
%% acknowledgements back perform no further action.
%%
%% 2. The other group members who upon receiving messages and
%% acknowledgements must update their own internal state accordingly
%% (the sending member must also do this in order to be able to
%% accomodate failures).
%%
%% We also have three distinct failure scenarios: imagine the chain of
%% A -> B -> C:
%%
%% 1. If B dies then A must contact C and directly tell C the messages
%% which are unacknowledged and the last acknowledgement seen. This is
%% the normal case for the failure of intermediate members. The list
%% of unacknowledged messages C receives from A may be equal to or a
%% suffix of the list of unacknowledged messages that C knows of:
%% acknowledgements are found from messages C knows of at the head of
%% the list which A has removed, whilst additional publishes are
%% present at the end of A's list which are absent from C's. Thus if
%% C's list of unacknowledged messages is [1,2,3] and the list it
%% receives from A is [3,4] then it knows that it should acknowledge 1
%% and 2, and publish 4.
%%
%% 2. If C dies then A will receive from B all the unacknowledged
%% messages as known by B. However, A, as the sender of messages, must
%% calculate from this the messages B is therefore implying A was
%% about to receive from C, and for which A was then about to issue
%% acknowledgements for. If A has a list of messages pending
%% acknowledgement of [4,5,6] and it receives from B the list of
%% [3,4,5] then it means: A has already sent out the acknowledgement
%% for 3, but it hasn't yet made it to B (no action to take for this -
%% A needs to take no actions concerning acknowledgements that were
%% lost (which C was about to send back to A but failed to do so as C
%% died) - A would have taken no action upon receipt of these messages
%% had they come from C); and that the publication of messages 4 and 5
%% made it to B, but A is yet to acknowledge these messages, and so A
%% should now send out the acknowledgements for messages 4 and 5,
%% reducing its list of messages pending acknowledgement to just
%% [6].
%%
%% 3. If A dies then B must now take responsibility for the actions
%% that A would have performed upon receiving its own messages back
%% (i.e. converting them to acknowledgements and sending them on),
%% plus not sending on any acknowledgements it receives. I.e. B is now
%% in charge of the messages that were still alive when A died. The
%% same scenarios are valid here as in case (2) above: B is sure to
%% know at least as much as any one else about the messages that A
%% sent. Correspondingly, the detection of acknowledgements and
%% publications are the same: B can simply pretend that it sent the
%% messages.
%%
%% In the event of a member joining the chain, they can join at any
%% location within the chain. Their upstream member will send them the
%% unacknowledged messages, which the new member will update its own
%% state with, interpret the messages unacknowledged, but not forward
%% such messages on (as the nearest downstream member would already
%% have been sent such messages).
%%
%% In the example chain A -> B -> C, care must be taken in the event
%% of the death of B, that C does not process any messages it receives
%% from B that were in flight but unreceived at the point of the death
%% of B, _after_ it has established contact from A.
%%
%% Finally, we abstract all the above so that any member of the group
%% can send messages: thus all group members are equal and can
%% simultaneously play the roles of A, B or C from the above
%% description depending solely on whether they or someone else sent
%% each message.
%%
%% Obvious extension points:
%%
%% 1. When sending a message, indicate which members of the group the
%% message is intended for. Everything proceeds as above: the only
%% change is that members not in the recipients list do not invoke
%% their callback for such messages.
%%
%% 2. Expose membership changes through the callback.
%%


-behaviour(gen_server2).

-export([create_table/0, join_group/2, leave_group/1, broadcast/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TABLE, gm_group).
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(state,
        { self,
          upstream,
          downstream,
          group_name,
          members,
          callback,
          pub_count,
          myselves
        }).

-record(gm_group, { name, members }).

create_table() ->
    case mnesia:create_table(
           ?TABLE, [{record_name, gm_group},
                    {attributes, record_info(fields, gm_group)}]) of
        {atomic, ok}                          -> ok;
        {aborted, {already_exists, gm_group}} -> ok;
        Err                                   -> Err
    end.

join_group(GroupName, Callback) ->
    gen_server2:start_link(?MODULE, [GroupName, Callback], []).

leave_group(Server) ->
    gen_server2:cast(Server, leave).

broadcast(Server, Msg) ->
    gen_server2:cast(Server, {broadcast, Msg}).

init([GroupName, Callback]) ->
    process_flag(trap_exit, true),
    gen_server2:cast(self(), join_group),
    Self = self(),
    {ok, #state { self        = Self,
                  upstream    = undefined,
                  downstream  = undefined,
                  group_name  = GroupName,
                  members     = undefined,
                  callback    = Callback,
                  pub_count   = 0,
                  myselves    = sets:add_element(Self, sets:new()) }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({add_new_member, Member}, _From,
            State = #state { self       = Self,
                             group_name = GroupName,
                             members    = Members }) ->
    {atomic, Result} =
        mnesia:sync_transaction(
          fun () ->
                  [Group = #gm_group { members = Members1 }] =
                      mnesia:read(?TABLE, GroupName),
                  Q = queue:from_list(Members1),
                  case queue:out_r(Q) of
                      {{value, Self}, _Q1} ->
                          Q1 = queue:to_list(queue:in(Member, Q)),
                          Group1 = Group #gm_group { members = Q1 },
                          ok = mnesia:write(Group1),
                          {ok, Group1};
                      {{value, _Member}, _Q1} ->
                          not_most_recently_added
                  end
          end),
    case Result of
        {ok, Group2} ->
            {reply, {ok, Group2, Members}, check_neighbours(Group2, State)};
        not_most_recently_added ->
            {reply, not_most_recently_added, State}
    end.

handle_cast({activity, Up, Msgs}, State = #state { self       = Self,
                                                   upstream   = {Up, _MRef},
                                                   downstream = Down,
                                                   members    = Members,
                                                   callback   = Callback,
                                                   myselves   = Myselves }) ->
    ActivityFun = process_activity_fun(Myselves),
    {Members1, Outbound} = lists:foldl(ActivityFun, {Members, []}, Msgs),
    ok = send_downstream(Self, Outbound, Down),
    ok = callback(Callback, Outbound),
    {noreply, State #state { members = Members1 }};

handle_cast({activity, _Up, _Msgs}, State) ->
    {noreply, State};

handle_cast({broadcast, Msg}, State = #state { self       = Self,
                                               downstream = Down,
                                               members    = Members,
                                               pub_count  = PubCount }) ->
    PubMsg = {PubCount, Msg},
    Outbound = [{Self, [PubMsg], []}],
    ok = send_downstream(Self, Outbound, Down),
    Members1 = with_member(fun (PA) -> queue:in(PubMsg, PA) end, Self, Members),
    {noreply, State #state { members = Members1, pub_count = PubCount + 1 }};

handle_cast(leave_group, State) ->
    {stop, normal, State};

handle_cast(join_group, State = #state { self       = Self,
                                         upstream   = undefined,
                                         downstream = undefined,
                                         group_name = GroupName,
                                         members    = undefined,
                                         callback   = Callback }) ->
    {Group, Members} =
        case maybe_create_group(GroupName, Self) of
            {new, Group1}      -> {Group1, dict:new()};
            {existing, Group1} -> internal_join_group(Self, Group1)
        end,
    {Up, Down} = find_neighbours(Group, Self),
    State1 = State #state { upstream   = {Up, maybe_monitor(Up, Self)},
                            downstream = {Down, maybe_monitor(Down, Self)},
                            members    = Members },
    ok = callback(
           Callback,
           dict:fold(
             fun (Id, PendingAcks, MsgsAcc) ->
                     output_cons(MsgsAcc, Id, queue:to_list(PendingAcks), [])
             end, [], Members)),
    {noreply, State1};

handle_cast(check_neighbours, State = #state { group_name  = GroupName }) ->
    {ok, Group} = read_group(GroupName),
    {noreply, check_neighbours(Group, State)};

handle_cast({catch_up, Up, MembersUp},
            State = #state { self       = Self,
                             upstream   = {Up, _MRef},
                             downstream = Down,
                             members    = Members,
                             callback   = Callback,
                             myselves   = Myselves }) ->
    AllMembers = lists:usort(
                   dict:fetch_keys(Members) ++ dict:fetch_keys(MembersUp)),
    {Members1, Outbound} =
        lists:foldl(
          fun (Id, MembersOutbound) ->
                  PAUp = find_member_or_blank(Id, MembersUp),
                  with_member_acc(
                    fun (PA, Outbound1) ->
                            case sets:is_element(Id, Myselves) of
                                true ->
                                    {_AcksInFlight, Pubs, PA1} =
                                        find_prefix_common_suffix(PAUp, PA),
                                    {PA1, output_cons(Outbound1, Id, [],
                                                      acks_from_queue(Pubs))};
                                false ->
                                    {Acks, Common, Pubs} =
                                        find_prefix_common_suffix(PA, PAUp),
                                    {queue:join(Common, Pubs),
                                     output_cons(Outbound1, Id,
                                                 queue:to_list(Pubs),
                                                 acks_from_queue(Acks))}
                            end
                    end, Id, MembersOutbound)
          end, {Members, []}, AllMembers),
    ok = send_downstream(Self, Outbound, Down),
    ok = callback(Callback, Outbound),
    {noreply, State #state { members = Members1 }};

handle_cast({catch_up, _Up, _MembersUp}, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, _Reason},
            State = #state { self       = Self,
                             upstream   = Up,
                             downstream = Down,
                             group_name = GroupName,
                             members    = Members,
                             myselves   = Myselves }) ->
    Member = case {Up, Down} of
                 {{Member1, MRef}, _} -> Member1;
                 {_, {Member1, MRef}} -> Member1;
                 _                    -> unknown
             end,
    Myselves1 = case Up of
                    {Member, MRef} -> sets:add_element(Member, Myselves);
                    _              -> Myselves
                end,
    Group =
        case Member of
            unknown ->
                {ok, Group1} = read_group(GroupName),
                Group1;
            _ ->
                {atomic, Group1} =
                    mnesia:sync_transaction(
                      fun () ->
                              [Group2 = #gm_group { members = Members1 }] =
                                  mnesia:read(?TABLE, GroupName),
                              case lists:delete(Member, Members1) of
                                  Members1 -> Group2;
                                  Members2 -> Group3 = Group2 #gm_group {
                                                         members = Members2 },
                                              mnesia:write(Group3),
                                              Group3
                              end
                      end),
                Group1
        end,
    State1 = #state { downstream = Down1 } =
        check_neighbours(Group, State #state { myselves = Myselves1 }),
    ok = case Down1 of
             Down               -> ok;
             {Self, undefined}  -> ok;
             {Neighbour, _MRef} -> gen_server2:cast(Neighbour,
                                                    {catch_up, Self, Members})
         end,
    {noreply, State1}.

terminate(_Reason, _State) ->
    io:format("~p death ~p ~p~n", [self(), _Reason, _State]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ---------------------------------------------------------------------------
%% Group membership management
%% ---------------------------------------------------------------------------

find_neighbours(#gm_group { members = Members }, Self) ->
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

read_group(GroupName) ->
    case mnesia:dirty_read(?TABLE, GroupName) of
        []      -> {error, not_found};
        [Group] -> {ok, Group}
    end.

maybe_create_group(GroupName, Self) ->
    case read_group(GroupName) of
        {error, not_found} ->
            {atomic, Result} =
                mnesia:sync_transaction(
                  fun () ->
                          case mnesia:read(?TABLE, GroupName) of
                              [] ->
                                  Group = #gm_group{ name = GroupName,
                                                     members = [Self] },
                                  ok = mnesia:write(Group),
                                  {new, Group};
                              [Group] ->
                                  {existing, Group}
                          end
                  end),
            Result;
        {ok, Group} ->
            {existing, Group}
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

internal_join_group(Self, #gm_group { name = GroupName, members = Members }) ->
    [MostRecentlyAdded | _] = lists:reverse(Members),
    case gen_server2:call(
           MostRecentlyAdded, {add_new_member, Self}, infinity) of
        {ok, Group, Members1}   -> {Group, Members1};
        not_most_recently_added -> {ok, Group} = read_group(GroupName),
                                   internal_join_group(Self, Group)
    end.

check_neighbours(Group, State = #state { self       = Self,
                                         upstream   = Up,
                                         downstream = Down }) ->
    {Up1, Down1} = find_neighbours(Group, Self),
    Up2 = ensure_neighbour(Self, Up, Up1),
    Down2 = ensure_neighbour(Self, Down, Down1),
    State #state { upstream = Up2, downstream = Down2 }.

%% ---------------------------------------------------------------------------
%% Catchup delta detection
%% ---------------------------------------------------------------------------

find_prefix_common_suffix(A, B) ->
    {Prefix, A1} = find_prefix(A, B, queue:new()),
    {Common, Suffix} = find_common(A1, B, queue:new()),
    {Prefix, Common, Suffix}.

%% Returns the elements of A that occur before the first element of B,
%% plus the remainder of A.
find_prefix(A, B, Prefix) ->
    case {queue:out(A), queue:out(B)} of
        {{{value, Val}, _A1}, {{value, Val}, _B1}} ->
            {Prefix, A};
        {{empty, A1}, {{value, _A}, _B1}} ->
            {Prefix, A1};
        {{{value, {NumA, _MsgA} = Val}, A1},
         {{value, {NumB, _MsgB}}, _B1}} when NumA < NumB ->
            find_prefix(A1, B, queue:in(Val, Prefix));
        {_, {empty, _B1}} ->
            {A, Prefix} %% Prefix well be empty here
    end.

%% A should be a prefix of B. Returns the commonality plus the
%% remainder of B.
find_common(A, B, Common) ->
    case {queue:out(A), queue:out(B)} of
        {{{value, Val}, A1}, {{value, Val}, B1}} ->
            find_common(A1, B1, queue:in(Val, Common));
        {{empty, _A}, _} ->
            {Common, B}
    end.

%% ---------------------------------------------------------------------------
%% Sending
%% ---------------------------------------------------------------------------

output_cons(Outbound, _Id, [], []) ->
    Outbound;
output_cons(Outbound, Id, Pubs, Acks) ->
    [{Id, Pubs, Acks} | Outbound].

send_downstream(Self, _Msg, {Self, undefined}) ->
    ok;
send_downstream(_Self, [], _Down) ->
    ok;
send_downstream(Self, Msg, {Down, _MRef}) ->
    gen_server2:cast(Down, {activity, Self, Msg}).

callback(Callback, Msgs) ->
    [Callback(Id, Pub) || {Id, Pubs, _Acks} <- Msgs, {_PubNum, Pub} <- Pubs],
    ok.

%% ---------------------------------------------------------------------------
%% Members helpers
%% ---------------------------------------------------------------------------

with_member(Fun, Id, Members) ->
    maybe_store_member(Id, Fun(find_member_or_blank(Id, Members)), Members).

with_member_acc(Fun, Id, {Members, Acc}) ->
    {Member1, Acc1} = Fun(find_member_or_blank(Id, Members), Acc),
    {maybe_store_member(Id, Member1, Members), Acc1}.

find_member_or_blank(Id, Members) ->
    case dict:find(Id, Members) of
        {ok, Result} -> Result;
        error        -> blank_member()
    end.

blank_member() ->
    queue:new().

is_member_empty(Member) ->
    queue:is_empty(Member).

maybe_store_member(Id, Member, Members) ->
    case is_member_empty(Member) of
        true  -> dict:erase(Id, Members);
        false -> dict:store(Id, Member, Members)
    end.

%% ---------------------------------------------------------------------------
%% Msg transformation
%% ---------------------------------------------------------------------------

acks_from_queue(Pubs) ->
    [PubNum || {PubNum, _Msg} <- queue:to_list(Pubs)].

apply_acks([], Pubs) ->
    Pubs;
apply_acks([PubNum | Acks], Pubs) ->
    {{value, {PubNum, _Msg}}, Pubs1} = queue:out(Pubs),
    apply_acks(Acks, Pubs1).

process_activity_fun(Myselves) ->
    fun ({Id, Pubs, Acks} = Msg, MembersOutbound) ->
            with_member_acc(
              fun (PA, Outbound1) ->
                      case sets:is_element(Id, Myselves) of
                          true ->
                              {ToAck, PA1} = find_common(queue:from_list(Pubs),
                                                         PA, queue:new()),
                              {PA1, output_cons(Outbound1, Id, [],
                                                acks_from_queue(ToAck))};
                          false ->
                              PA1 = queue:join(PA, queue:from_list(Pubs)),
                              {apply_acks(Acks, PA1), [Msg | Outbound1]}
                      end
              end, Id, MembersOutbound)
    end.
