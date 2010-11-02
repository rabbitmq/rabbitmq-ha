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

-behaviour(gen_server2).

-export([create_tables/0, join/2, leave/1, broadcast/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(GROUP_TABLE, gm_group).
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(state,
        { self,
          left,
          right,
          group_name,
          members_state,
          callback,
          pub_count,
          myselves
        }).

-record(gm_group, { name, members }).

-define(TABLES,
        [{?GROUP_TABLE, [{record_name, gm_group},
                         {attributes, record_info(fields, gm_group)}]}
        ]).

create_tables() ->
    create_tables(?TABLES).

create_tables([]) ->
    ok;
create_tables([{Table, Attributes} | Tables]) ->
    case mnesia:create_table(Table, Attributes) of
        {atomic, ok}                          -> create_tables(Tables);
        {aborted, {already_exists, gm_group}} -> create_tables(Tables);
        Err                                   -> Err
    end.

join(GroupName, Callback) ->
    gen_server2:start_link(?MODULE, [GroupName, Callback], []).

leave(Server) ->
    gen_server2:cast(Server, leave).

broadcast(Server, Msg) ->
    gen_server2:cast(Server, {broadcast, Msg}).

init([GroupName, Callback]) ->
    process_flag(trap_exit, true),
    gen_server2:cast(self(), join),
    Self = self(),
    {ok, #state { self          = Self,
                  left          = undefined,
                  right         = undefined,
                  group_name    = GroupName,
                  members_state = undefined,
                  callback      = Callback,
                  pub_count     = 0,
                  myselves      = sets:add_element(Self, sets:new()) },
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({add_new_member, Member}, _From,
            State = #state { self          = Self,
                             group_name    = GroupName,
                             members_state = MembersState }) ->
    {atomic, Result} =
        mnesia:sync_transaction(
          fun () ->
                  [Group = #gm_group { members = Members }] =
                      mnesia:read(?GROUP_TABLE, GroupName),
                  Q = queue:from_list(Members),
                  case queue:out_r(Q) of
                      {{value, Self}, _Q1} ->
                          Members1 = queue:to_list(queue:in(Member, Q)),
                          Group1 = Group #gm_group { members = Members1 },
                          mnesia:write(Group1),
                          {ok, Group1};
                      {{value, _Member}, _Q1} ->
                          not_most_recently_added
                  end
          end),
    case Result of
        {ok, Group2} ->
            reply({ok, Group2, MembersState}, check_neighbours(Group2, State));
        not_most_recently_added ->
            reply(not_most_recently_added, State)
    end.

handle_cast({activity, Left, Msgs},
            State = #state { self          = Self,
                             left          = {Left, _MRef},
                             right         = Right,
                             members_state = MembersState,
                             callback      = Callback,
                             myselves      = Myselves }) ->
    ActivityFun = process_activity_fun(Myselves),
    {MembersState1, Outbound} =
        lists:foldl(ActivityFun, {MembersState, []}, Msgs),
    send_right(Self, Outbound, Right),
    ok = callback(Callback, Outbound),
    noreply(State #state { members_state = MembersState1 });

handle_cast({activity, _Left, _Msgs}, State) ->
    noreply(State);

handle_cast({broadcast, Msg}, State) ->
    noreply(broadcast_internal(Msg, State));

handle_cast(leave, State) ->
    {stop, normal, State};

handle_cast(join, State = #state { self          = Self,
                                   left          = undefined,
                                   right         = undefined,
                                   group_name    = GroupName,
                                   members_state = undefined,
                                   callback      = Callback }) ->
    {Group, MembersState} =
        join_group_internal(Self, maybe_create_group(GroupName, Self)),
    {Left, Right} = find_neighbours(Group, Self),
    State1 = State #state { left          = {Left, maybe_monitor(Left, Self)},
                            right         = {Right, maybe_monitor(Right, Self)},
                            members_state = MembersState },
    State2 = broadcast_internal({member_joined, Self}, State1),
    ok = callback(
           Callback,
           dict:fold(
             fun (Id, PendingAcks, MsgsAcc) ->
                     output_cons(MsgsAcc, Id, queue:to_list(PendingAcks), [])
             end, [], MembersState)),
    noreply(State2);

handle_cast(check_neighbours, State = #state { group_name  = GroupName }) ->
    noreply(check_neighbours(read_group(GroupName), State));

handle_cast({catch_up, Left, MembersStateLeft},
            State = #state { self          = Self,
                             left          = {Left, _MRef},
                             right         = Right,
                             members_state = MembersState,
                             callback      = Callback,
                             myselves      = Myselves }) ->
    AllMembers = lists:usort(dict:fetch_keys(MembersState) ++
                                 dict:fetch_keys(MembersStateLeft)),
    {MembersState1, Outbound} =
        lists:foldl(
          fun (Id, MembersStateOutbound) ->
                  PALeft = find_member_or_blank(Id, MembersStateLeft),
                  with_member_acc(
                    fun (PA, Outbound1) ->
                            case sets:is_element(Id, Myselves) of
                                true ->
                                    {_AcksInFlight, Pubs, PA1} =
                                        find_prefix_common_suffix(PALeft, PA),
                                    {PA1, output_cons(Outbound1, Id, [],
                                                      acks_from_queue(Pubs))};
                                false ->
                                    {Acks, Common, Pubs} =
                                        find_prefix_common_suffix(PA, PALeft),
                                    {join_pubs(Common, queue:to_list(Pubs)),
                                     output_cons(Outbound1, Id,
                                                 queue:to_list(Pubs),
                                                 acks_from_queue(Acks))}
                            end
                    end, Id, MembersStateOutbound)
          end, {MembersState, []}, AllMembers),
    send_right(Self, Outbound, Right),
    ok = callback(Callback, Outbound),
    noreply(State #state { members_state = MembersState1 });

handle_cast({catch_up, _Left, _MembersStateLeft}, State) ->
    noreply(State).

handle_info({'DOWN', MRef, process, _Pid, _Reason},
            State = #state { self          = Self,
                             left          = Left,
                             right         = Right,
                             group_name    = GroupName,
                             members_state = MembersState,
                             callback      = Callback }) ->
    Member = case {Left, Right} of
                 {{Member1, MRef}, _} -> Member1;
                 {_, {Member1, MRef}} -> Member1;
                 _                    -> unknown
             end,
    Group = #gm_group {} =
        case Member of
            unknown -> read_group(GroupName);
            _       -> delete_member_from_group(Member, GroupName)
        end,
    State1 = #state { right = Right1 } = check_neighbours(Group, State),
    State2 = case Right1 of
                 Right ->
                     State1;
                 {Self, undefined} ->
                     State1 #state { members_state = blank_member_state() };
                 {Neighbour, _MRef} ->
                     ok = gen_server2:cast(
                            Neighbour, {catch_up, Self, MembersState}),
                     State1
             end,
    noreply(case Left of %% original left
                {Member, _MRef2} -> Msg = {member_left, Member},
                                    Callback(Self, Msg),
                                    broadcast_internal(Msg, State2);
                _                -> State2
            end).

terminate(normal, _State) ->
    ok;
terminate(Reason, State) ->
    io:format("~p died~nreason: ~p~nstate: ~p~n", [self(), Reason, State]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

%% ---------------------------------------------------------------------------
%% Group membership management
%% ---------------------------------------------------------------------------

find_neighbours(#gm_group { members = Members }, Self) ->
    find_neighbours1(queue:from_list(Members), Self).

find_neighbours1(Q, Self) ->
    case queue:out(Q) of
        {{value, Self}, Q1} ->
            {{value, Left}, _Q2} = queue:out_r(Q),
            {{value, Right}, _Q3} = queue:out(queue:in(Self, Q1)),
            {Left, Right};
        {{value, Other}, Q1} ->
            find_neighbours1(queue:in(Other, Q1), Self)
    end.

read_group(GroupName) ->
    case mnesia:dirty_read(?GROUP_TABLE, GroupName) of
        []      -> {error, not_found};
        [Group] -> Group
    end.

maybe_create_group(GroupName, Self) ->
    case read_group(GroupName) of
        Group = #gm_group { members = [_|_] } ->
            Group;
        Case when Case =:= {error, not_found} orelse
                  Case #gm_group.members =:= [] ->
            {atomic, Group} =
                mnesia:sync_transaction(
                  fun () ->
                          case mnesia:read(?GROUP_TABLE, GroupName) of
                              [Group1 = #gm_group { members = [_|_] }] ->
                                  Group1;
                              _ ->
                                  Group1 = #gm_group { name    = GroupName,
                                                       members = [Self] },
                                  mnesia:write(Group1),
                                  Group1
                          end
                  end),
            Group
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

delete_member_from_group(Member, GroupName) ->
    {atomic, Group} =
        mnesia:sync_transaction(
          fun () -> [Group = #gm_group { members = Members }] =
                        mnesia:read(?GROUP_TABLE, GroupName),
                    case lists:delete(Member, Members) of
                        Members  -> Group;
                        Members1 -> Group1 =
                                        Group #gm_group { members = Members1 },
                                    mnesia:write(Group1),
                                    Group1
                    end
          end),
    Group.

join_group_internal(Self, #gm_group { members = [], name = GroupName }) ->
    join_group_internal(Self, maybe_create_group(GroupName, Self));
join_group_internal(Self, #gm_group { members = [Self] } = Group) ->
    {Group, blank_member_state()};
join_group_internal(Self, #gm_group { name = GroupName, members = Members }) ->
    [MostRecentlyAdded | _] = lists:reverse(Members),
    try
        case gen_server2:call(
               MostRecentlyAdded, {add_new_member, Self}, infinity) of
            {ok, Group, MembersState} ->
                {Group, MembersState};
            not_most_recently_added ->
                join_group_internal(Self, read_group(GroupName))
        end
    catch
        exit:{noproc, _} ->
            Group1 = #gm_group {} =
                case Members of
                    [MostRecentlyAdded] ->
                        delete_member_from_group(MostRecentlyAdded, GroupName);
                    _ ->
                        read_group(GroupName)
                end,
            join_group_internal(Self, Group1)
    end.

check_neighbours(Group = #gm_group {}, State = #state { self  = Self,
                                                        left  = Left,
                                                        right = Right }) ->
    {Left1, Right1} = find_neighbours(Group, Self),
    Left2 = ensure_neighbour(Self, Left, Left1),
    Right2 = ensure_neighbour(Self, Right, Right1),
    io:format("~p {~p, ~p}~n", [Self, Left1, Right1]),
    State #state { left = Left2, right = Right2 }.

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

send_right(Self, _Msg, {Self, undefined}) ->
    false;
send_right(_Self, [], _Right) ->
    false;
send_right(Self, Msg, {Right, _MRef}) ->
    ok = gen_server2:cast(Right, {activity, Self, Msg}),
    true.

callback(Callback, Msgs) ->
    [Callback(Id, Pub) || {Id, Pubs, _Acks} <- Msgs, {_PubNum, Pub} <- Pubs],
    ok.

broadcast_internal(Msg, State = #state { self          = Self,
                                         right         = Right,
                                         members_state = MembersState,
                                         pub_count     = PubCount }) ->
    PubMsg = {PubCount, Msg},
    Outbound = [{Self, [PubMsg], []}],
    MembersState1 =
        case send_right(Self, Outbound, Right) of
            true  -> with_member(fun (PA) -> queue:in(PubMsg, PA) end,
                                 Self, MembersState);
            false -> MembersState
        end,
    State #state { members_state = MembersState1, pub_count = PubCount + 1 }.

%% ---------------------------------------------------------------------------
%% Members helpers
%% ---------------------------------------------------------------------------

with_member(Fun, Id, MembersState) ->
    maybe_store_member(
      Id, Fun(find_member_or_blank(Id, MembersState)), MembersState).

with_member_acc(Fun, Id, {MembersState, Acc}) ->
    {MemberState, Acc1} = Fun(find_member_or_blank(Id, MembersState), Acc),
    {maybe_store_member(Id, MemberState, MembersState), Acc1}.

find_member_or_blank(Id, MembersState) ->
    case dict:find(Id, MembersState) of
        {ok, Result} -> Result;
        error        -> blank_member()
    end.

blank_member() ->
    queue:new().

blank_member_state() ->
    dict:new().

is_member_empty(Member) ->
    queue:is_empty(Member).

maybe_store_member(Id, Member, MembersState) ->
    case is_member_empty(Member) of
        true  -> dict:erase(Id, MembersState);
        false -> dict:store(Id, Member, MembersState)
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
    fun ({Id, Pubs, Acks} = Msg, MembersStateOutbound) ->
            with_member_acc(
              fun (PA, Outbound1) ->
                      case sets:is_element(Id, Myselves) of
                          true ->
                              {ToAck, PA1} = find_common(queue:from_list(Pubs),
                                                         PA, queue:new()),
                              {PA1, output_cons(Outbound1, Id, [],
                                                acks_from_queue(ToAck))};
                          false ->
                              PA1 = join_pubs(PA, Pubs),
                              {apply_acks(Acks, PA1), [Msg | Outbound1]}
                      end
              end, Id, MembersStateOutbound)
    end.

join_pubs(Q, []) ->
    Q;
join_pubs(Q, [{Num, _} = Pub | Pubs]) ->
    case queue:out_r(Q) of
        {empty, _Q} ->
            join_pubs(queue:in(Pub, Q), Pubs);
        {{value, {Num1, _}}, _Q} when Num1 + 1 =:= Num ->
            join_pubs(queue:in(Pub, Q), Pubs)
    end.
