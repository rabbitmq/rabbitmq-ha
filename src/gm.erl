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
          callback,
          view,
          pub_count,
          members_state
        }).

-record(gm_group, { name, members }).

-record(view_member, { id, aliases, left, right }).

-define(TAG, '$gm').

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
    ok. %%gen_server2:cast(Server, {broadcast, Msg}).

init([GroupName, Callback]) ->
    process_flag(trap_exit, true),
    random:seed(now()),
    gen_server2:cast(self(), join),
    Self = self(),
    {ok, #state { self          = Self,
                  left          = {Self, undefined},
                  right         = {Self, undefined},
                  group_name    = GroupName,
                  callback      = Callback,
                  view          = undefined,
                  pub_count     = 0,
                  members_state = blank_member_state() }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_msg, Msg}, State}.

handle_cast(A = {activity, {From, To, Msgs}}, State) ->
    io:format("~p ~p~n", [self(), A]),
    State1 = #state { self          = Self,
                      right         = Right,
                      view          = View,
                      members_state = MembersState } =
        freshen_view(From, To, Msgs, State),
    case is_alive(From, View) of
        true ->
            MemberSelf = view_fetch(Self, View),
            {MembersState2, Output1} =
                lists:foldl(
                  fun ({Id, Pubs, Acks}, MembersStateOutput) ->
                          with_member_acc(
                            fun (PA, Output2) ->
                                    case is_alias_of(Id, MemberSelf) of
                                        true ->
                                            {ToAck, PA1} =
                                                find_common(
                                                  queue:from_list(Pubs), PA,
                                                  queue:new()),
                                            {PA1, output_cons(
                                                    Id, [],
                                                    acks_from_queue(ToAck),
                                                    Output2)};
                                        false ->
                                            {apply_acks(Acks,
                                                        join_pubs(PA, Pubs)),
                                             output_cons(Id, Pubs, Acks,
                                                         Output2)}
                                    end
                            end, Id, MembersStateOutput)
                  end, {MembersState, output_nil()}, Msgs),
            send_right(Self, View, Right, Output1),
            noreply(State1 #state { members_state = MembersState2 });
        false ->
            noreply(State1)
    end;

handle_cast({catch_up, From, To, MembersStateFrom, ViewFrom}, State) ->
    State1 = #state { self          = Self,
                      view          = View,
                      members_state = MembersState } =
        freshen_view(From, To, [], State #state { view = ViewFrom }),
    case is_alive(From, View) of
        true ->
            MemberSelf = view_fetch(Self, View),
            AllMembers = lists:usort(dict:fetch_keys(MembersState) ++
                                         dict:fetch_keys(MembersStateFrom)),
            {MembersState1, Output} =
                lists:foldl(
                  fun (Id, MembersStateOutput) ->
                          PAFrom = find_member_or_blank(Id, MembersStateFrom),
                          with_member_acc(
                            fun (PA, Output1) ->
                                    {PA,
                                     case is_alias_of(Id, MemberSelf) of
                                         true ->
                                             {_AcksInFlight, Pubs, _PA1} =
                                                 find_prefix_common_suffix(
                                                   PAFrom, PA),
                                             output_cons(Id, [],
                                                         acks_from_queue(Pubs),
                                                         Output1);
                                         false ->
                                             {Acks, _Common, Pubs} =
                                                 find_prefix_common_suffix(
                                                   PA, PAFrom),
                                             output_cons(Id,
                                                         queue:to_list(Pubs),
                                                         acks_from_queue(Acks),
                                                         Output1)
                                     end}
                            end, Id, MembersStateOutput)
                  end, {MembersState, output_nil()}, AllMembers),
            io:format("~p calculated from catch_up ~p~n", [Self, output_finalise(Output)]),
            handle_cast({activity, {From, To, output_finalise(Output)}},
                        State1 #state { members_state = MembersState1 });
        false ->
            noreply(State1)
    end;

handle_cast(leave, State) ->
    {stop, normal, State};

%% Link Layer
handle_cast(join, State = #state { self       = Self,
                                   left       = {Self, undefined},
                                   right      = {Self, undefined},
                                   group_name = GroupName,
                                   view       = undefined }) ->
    Group = join_group_internal(Self, GroupName),
    View = group_to_view(Group),
    State1 = check_neighbours(Group, State #state { view = View }),
    noreply(internal_broadcast(
              [{?TAG, {birth_of, Self, view_successor(Self, View)}}], State1));

%% Link Layer
handle_cast(check_neighbours, State = #state { group_name  = GroupName }) ->
    noreply(check_neighbours(read_group(GroupName), State)).

%% Link Layer
handle_info({'DOWN', MRef, process, _Pid, _Reason},
            State = #state { left       = Left,
                             right      = Right,
                             group_name = GroupName }) ->
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
    noreply(check_neighbours(Group, State)).

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
%% Group membership management - Link Layer
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

join_group_internal(Self, GroupName) ->
    {atomic, Group} =
        mnesia:sync_transaction(
          fun () ->
                  Group1 =
                      case mnesia:read(?GROUP_TABLE, GroupName) of
                          [Group2 = #gm_group { members = [_|_] = Members }] ->
                              {A, B} =
                                  lists:split(
                                    random:uniform(length(Members)) - 1,
                                    Members),
                              Group2 #gm_group { members = A ++ [Self | B] };
                          _ ->
                              #gm_group { name    = GroupName,
                                          members = [Self] }
                      end,
                  mnesia:write(Group1),
                  Group1
          end),
    Group.

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

check_neighbours(Group = #gm_group {},
                 State = #state { self      = Self,
                                  left      = Left,
                                  right     = Right }) ->
    {Left1, Right1} = find_neighbours(Group, Self),
    Left2 = ensure_neighbour(Self, Left, Left1),
    Right2 = ensure_neighbour(Self, Right, Right1),
    %% io:format("~p >--> ~p >--> ~p~n", [Left1, Self, Right1]),
    State #state { left = Left2, right = Right2 }.

%% ---------------------------------------------------------------------------
%% View
%% ---------------------------------------------------------------------------

group_to_view(#gm_group { members = Members }) ->
    group_to_view(Members ++ Members ++ Members, dict:new()).

group_to_view([Left, Self, Right | Rest], View) ->
    case dict:find(Self, View) of
        error ->
            group_to_view(
              [Self, Right | Rest],
              store_view_member(#view_member { id      = Self,
                                               aliases = sets:new(),
                                               left    = Left,
                                               right   = Right }, View));
        {ok, _} ->
            View
    end;
group_to_view(_, View) ->
    View.

store_view_members(Members, View) ->
    lists:foldl(fun store_view_member/2, View, Members).

store_view_member(Member = #view_member { id = Id }, View) ->
    View1 = dict:store(Id, Member, View),
    Key = {requires, Id},
    case dict:find(Key, View1) of
        error      -> View1;
        {ok, Msgs} -> process_view_msgs1(Msgs, dict:erase(Key, View1))
    end.

find_members_inclusive(Start, End, View) ->
    find_members_inclusive(View, Start, End, []).

find_members_inclusive(View, End, End, Acc) ->
    [dict:fetch(End, View) | Acc];
find_members_inclusive(View, Start, End, Acc) ->
    Member = #view_member { right = Right } = dict:fetch(Start, View),
    find_members_inclusive(View, Right, End, [Member | Acc]).

view_successor(Id, View) ->
    (dict:fetch(Id, View)) #view_member.right.

view_fetch(Id, View) ->
    dict:fetch(Id, View).

is_alive(Id, View) ->
    case view_fetch(Id, View) of
        #view_member {} -> true;
        _               -> false
    end.

freshen_view(From, To, Msgs, State = #state { self          = Self,
                                              right         = {Right, _MRef},
                                              view          = View,
                                              members_state = MembersState }) ->
    %% First, process all explicit view alterations. Idempotent.
    View1 = process_view_msgs(Msgs, View),
    %% Now try to detect anything else that may have happened: new
    %% aliases which affect our subsequent handling.
    {Deaths, View2} = update_view(From, To, Self, View1),
    %% Detect if our Right has changed, and if so send it a catch_up:
    RightOld = view_successor(Self, View),
    case view_successor(Self, View2) of
        RightOld ->
            ok;
        RightNew when Right =/= Self ->
            gen_server2:cast(Right,
                             {catch_up, Self, RightNew, MembersState, View2})
    end,
    io:format("~s~n", [draw_view(Self, View2)]),
    %% Now send out any deaths we detected
    internal_broadcast([{?TAG, {death_of, Id}} || Id <- Deaths],
                       State #state { view = View2 }).

update_view(From, Self, Self, View) ->
    %% The sender was sure it was sending to us, so therefore it has
    %% been told about any changes that have happened.
    {[],
     case dict:fetch(Self, View) of
         #view_member { left = From } ->
             View;
         #view_member { left = Left, aliases = Aliases } = MemberSelf ->
             %% "From" is sure I'm after it. But it may be aware of
             %% births and deaths I'm not.
             case dict:find(From, View) of
                 error ->
                     %% If I don't know anything about From, then all
                     %% I can do is assume it's a new node to my left,
                     %% and I'll process deletions later.
                     case Left of
                         Self ->
                             MemberSelf1 =
                                 MemberSelf #view_member { left  = From,
                                                           right = From },
                             MemberFrom = #view_member { id      = From,
                                                         aliases = sets:new(),
                                                         left    = Self,
                                                         right   = Self },
                             store_view_members(
                               [MemberFrom, MemberSelf1], View);
                         _ ->
                             #view_member { right = Self } = MemberLeft =
                                 dict:fetch(Left, View),
                             MemberSelf1 =
                                 MemberSelf #view_member { left = From },
                             MemberLeft1 =
                                 MemberLeft #view_member { right = From },
                             MemberFrom = #view_member { id      = From,
                                                         aliases = sets:new(),
                                                         left    = Left,
                                                         right   = Self },
                             store_view_members(
                               [MemberFrom, MemberLeft1, MemberSelf1], View)
                     end;
                 {ok, #view_member { right = Right } = MemberLeft}
                   when Right =/= Self ->
                     %% We're likely, in the future, to be told the
                     %% following have died...
                     Dead = find_members_inclusive(Right, Left, View),
                     io:format("~p suspecting death (A) of ~p~n", [Self, Dead]),
                     {Aliases1, View1} =
                         become_aliases(Self, Dead, Aliases, View),
                     MemberSelf1 =
                         MemberSelf #view_member { left = From,
                                                   aliases = Aliases1 },
                     MemberLeft1 =
                         MemberLeft #view_member { right = Self },
                     store_view_members(
                       [MemberLeft1, MemberSelf1], View1)
             end
     end};
update_view(From, To, Self, View) ->
    %% The sender didn't realise we're on its right.
    case dict:fetch(Self, View) of
        #view_member { left = From, aliases = Aliases } ->
            %% No problem - it's either not received our birth yet; or
            %% it's not received notification of deaths of
            %% intermediates yet. Thus:
            true = ((dict:fetch(From, View)) #view_member.right =:= Self)
                orelse sets:is_element(To, Aliases), %% ASSERTION
            {[], View};
        #view_member { left = Left, aliases = Aliases } = MemberSelf ->
            case dict:find(From, View) of
                error ->
                    %% Looks like the new left is a new birth which
                    %% we're yet to hear about, and some deaths have
                    %% happened between it and us. Until we receive
                    %% the birth message from it, there's nothing we
                    %% can safely do.
                    {[], View};
                {ok, #view_member { right = Right } = MemberLeft} ->
                    %% Neither us, or our new mystery Left, expected
                    %% this to happen. Thus we need to detect deaths
                    %% on the left and broadcast them.
                    Dead = find_members_inclusive(Right, Left, View),
                    io:format("~p suspecting death (B) of ~p (from ~p to ~p) (~s)~n", [Self, Dead, Right, Left, draw_view(Self, View)]),
                    {Aliases1, View1} =
                        become_aliases(Self, Dead, Aliases, View),
                    MemberSelf1 =
                        MemberSelf #view_member { left    = From,
                                                  aliases = Aliases1 },
                    MemberLeft1 = MemberLeft #view_member { right = Self },
                    {[Member #view_member.id || Member <- Dead],
                     store_view_members([MemberLeft1, MemberSelf1], View1)}
            end
    end.

become_aliases(Owner, Members, Aliases, View) ->
    lists:foldl(
      fun (#view_member { id = Id, aliases = Ids },
           {AliasesAcc, ViewAcc}) ->
              {sets:union(Ids, sets:add_element(Id, AliasesAcc)),
               dict:store(Id, {alias_of, Owner}, ViewAcc)}
      end, {Aliases, View}, Members).

%% we only need to use resolve_member for Ids that come in from
%% messages: we guarantee that within our own view, left and right
%% links will never point to alias_of entries.
resolve_member(Id, View) ->
    case dict:find(Id, View) of
        error                          -> error;
        {ok, {alias_of, Id1}}          -> resolve_member(Id1, View);
        {ok, #view_member {} = Member} -> Member
    end.

is_alias_of(Id, #view_member { id = Id }) ->
    true;
is_alias_of(Id, #view_member { aliases = Aliases }) ->
    sets:is_element(Id, Aliases).

process_view_msgs(Msgs, View) ->
    lists:foldl(fun ({_From, Pubs, _Acks}, ViewAcc) ->
                        process_view_msgs1(Pubs, ViewAcc)
                end, View, Msgs).

%% Things we get guaranteed:
%% 1. We'll get a birth message before we see any other messages from that member
%% 2. We'll get a birth message before a death message of the same member
process_view_msgs1(Pubs, View) ->
    lists:foldl(
      fun ({_PubNum, {?TAG, {birth_of, Id, Right}} = Birth}, ViewAcc) ->
              io:format("~p birth_of ~p (--> ~p)~n", [self(), Id, Right]),
              case resolve_member(Id, View) of
                  error ->
                      case resolve_member(Right, ViewAcc) of
                          error ->
                              %% Sadly, there's the possibility that
                              %% we don't know about the Right: Id
                              %% knew of Right when it joined, but
                              %% we're yet to be told about it.
                              dict_cons({requires, Right}, Birth, ViewAcc);
                          #view_member { id = Self, left = Self, right = Self } =
                          MemberSelf ->
                              MemberNew = #view_member { id      = Id,
                                                         aliases = sets:new(),
                                                         left    = Self,
                                                         right   = Self },
                              MemberSelf1 =
                                  MemberSelf #view_member { left = Id,
                                                            right = Id },
                              store_view_members(
                                [MemberNew, MemberSelf1], ViewAcc);
                          #view_member { id = Right1, left = Left1 } =
                          MemberRight ->
                              %% if Right's already died, the correct
                              %% thing to do is to put our new member
                              %% on the left of whoever inherited
                              %% Right.
                              #view_member { right = Right1 } =
                                  MemberLeft = dict:fetch(Left1, ViewAcc),
                              MemberNew = #view_member { id      = Id,
                                                         aliases = sets:new(),
                                                         left    = Left1,
                                                         right   = Right1 },
                              MemberLeft1 = MemberLeft #view_member { right = Id },
                              MemberRight1 = MemberRight #view_member { left = Id },
                              store_view_members(
                                [MemberLeft1, MemberNew, MemberRight1], ViewAcc)
                      end;
                  #view_member {} ->
                      ViewAcc
              end;
          ({_PubNum, {?TAG, {death_of, Id}}}, ViewAcc) ->
              io:format("~p death_of ~p~n", [self(), Id]),
              case dict:fetch(Id, ViewAcc) of
                  {alias_of, _Id} ->
                      ViewAcc;
                  #view_member { right = Right } = MemberDead ->
                      #view_member { aliases = Aliases } = MemberRight =
                          dict:fetch(Right, ViewAcc),
                      {Aliases1, ViewAcc1} =
                          become_aliases(Right, [MemberDead], Aliases, ViewAcc),
                      MemberRight1 =
                          MemberRight #view_member { aliases = Aliases1 },
                      store_view_member(MemberRight1, ViewAcc1)
              end;
          (_Msg, ViewAcc) ->
              ViewAcc
      end, View, Pubs).

dict_cons(Key, Value, Dict) ->
    dict:update(Key, fun (List) -> [Value | List] end, [Value], Dict).

draw_view(Id, View) ->
    draw_view(View, Id, draw_member(View, Id, [])).

draw_view(_View, Stop, {Stop, Acc}) ->
    lists:flatten(Acc ++ io_lib:format("~p", [Stop]));
draw_view(View, Stop, {Id, Acc}) ->
    draw_view(View, Stop, draw_member(View, Id, Acc)).

draw_member(View, Id, Acc) ->
    #view_member { right = Right, aliases = Aliases } = view_fetch(Id, View),
    {Right, Acc ++ io_lib:format("(~p ~p) -> ", [Id, sets:to_list(Aliases)])}.

%% ---------------------------------------------------------------------------
%% Catch_up delta detection
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
%% Members helpers
%% ---------------------------------------------------------------------------

with_member(Fun, Id, MembersState) ->
    store_member(
      Id, Fun(find_member_or_blank(Id, MembersState)), MembersState).

with_member_acc(Fun, Id, {MembersState, Acc}) ->
    {MemberState, Acc1} = Fun(find_member_or_blank(Id, MembersState), Acc),
    {store_member(Id, MemberState, MembersState), Acc1}.

find_member_or_blank(Id, MembersState) ->
    case dict:find(Id, MembersState) of
        {ok, Result} -> Result;
        error        -> blank_member()
    end.

erase_member(Id, MembersState) ->
    dict:erase(Id, MembersState).

blank_member() ->
    queue:new().

blank_member_state() ->
    dict:new().

store_member(Id, Member, MembersState) ->
    dict:store(Id, Member, MembersState).

%% ---------------------------------------------------------------------------
%% Activity assembly
%% ---------------------------------------------------------------------------

output_nil() ->
    queue:new().

output_cons(_Id, [], [], Tail) ->
    Tail;
output_cons(Sender, Pubs, Acks, Tail) ->
    queue:in({Sender, Pubs, Acks}, Tail).

output_finalise(Activity) ->
    queue:to_list(Activity).

internal_broadcast(Msgs, State = #state { self          = Self,
                                          right         = Right,
                                          view          = View,
                                          pub_count     = PubCount,
                                          members_state = MembersState }) ->
    {PubMsgs, PubCount1} =
        lists:foldr(
          fun (Msg, {PubMsgsAcc, PubCountAcc}) ->
                  {[{PubCountAcc, Msg} | PubMsgsAcc], PubCountAcc + 1}
          end, {[], PubCount}, Msgs),
    MembersState1 =
        case send_right(Self, View, Right,
                        output_cons(Self, PubMsgs, [], output_nil())) of
            true ->
                with_member(
                  fun (PA) -> queue:join(PA, queue:from_list(PubMsgs)) end,
                  Self, MembersState);
            false ->
                MembersState
        end,
    State #state { members_state = MembersState1, pub_count = PubCount1 }.

send_right(Self, _View, {Self, undefined}, _Output) ->
    false;
send_right(Self, View, {Right, _MRef}, Output) ->
    case queue:is_empty(Output) of
        true  -> false;
        false -> ok = gen_server2:cast(
                        Right,
                        {activity, {Self, view_successor(Self, View),
                                    output_finalise(Output)}}),
                 true
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

%% TODO: when everything's right, this should just be queue:join/2
join_pubs(Q, []) ->
    Q;
join_pubs(Q, [{Num, _} = Pub | Pubs] = AllPubs) ->
    case queue:out_r(Q) of
        {empty, _Q} ->
            join_pubs(queue:in(Pub, Q), Pubs);
        {{value, {Num1, _}}, _Q} when Num1 + 1 =:= Num ->
            join_pubs(queue:in(Pub, Q), Pubs);
        {{value, {_Num1, _}}, _Q} ->
            io:format("~p join_pubs(~p, ~p)~n", [self(), queue:to_list(Q), AllPubs]),
            exit(join_pubs_failure)
    end.
