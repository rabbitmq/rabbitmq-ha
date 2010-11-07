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
          view
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
                  view          = undefined }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_msg, Msg}, State}.

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
    io:format("~p ~p~n", [Self, dict:to_list(View)]),
    noreply(check_neighbours(Group, State #state { view = View }));

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

check_neighbours(Group = #gm_group {}, State = #state { self  = Self,
                                                        left  = Left,
                                                        right = Right }) ->
    {Left1, Right1} = find_neighbours(Group, Self),
    Left2 = ensure_neighbour(Self, Left, Left1),
    Right2 = ensure_neighbour(Self, Right, Right1),
    io:format("~p >--> ~p >--> ~p~n", [Left1, Self, Right1]),
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
    dict:store(Id, Member, View).

find_members_inclusive(Start, End, View) ->
    find_members_inclusive(View, Start, End, []).

find_members_inclusive(_View, End, End, Acc) ->
    Acc;
find_members_inclusive(View, Start, End, Acc) ->
    Member = #view_member { right = Right } = dict:fetch(Start, View),
    find_members_inclusive(View, Right, End, [Member | Acc]).

update_view(From, Self, Self, View) ->
    %% The sender was sure it was sending to us, so therefore it has
    %% been told about any changes that have happened.
    case dict:fetch(Self, View) of
        #view_member { left = From } ->
            View;
        #view_member { left = Left, aliases = Aliases } = MemberSelf ->
            %% "From" is sure I'm after it. But it may be aware of
            %% deletions and additions I'm not.
            case dict:find(From, View) of
                error ->
                    %% If I don't know anything about From, then all I
                    %% can do is assume it's a new node to my left,
                    %% and I'll process deletions later.
                    #view_member { right = Self } = MemberLeft =
                        dict:fetch(Left, View),
                    MemberSelf1 = MemberSelf #view_member { left = From },
                    MemberLeft1 = MemberLeft #view_member { right = From },
                    MemberFrom = #view_member { id      = From,
                                                aliases = sets:new(),
                                                left    = Left,
                                                right   = Self },
                    store_view_members(
                      [MemberFrom, MemberLeft1, MemberSelf1], View);
                {ok, #view_member { right = Right } = MemberLeft}
                  when Right =/= MemberSelf ->
                    %% We're likely, in the future, to be told the
                    %% following have died...
                    Dead = find_members_inclusive(Right, Left, View),
                    {Aliases1, View1} =
                        lists:foldl(
                          fun (#view_member { id = Id, aliases = Ids },
                               {AliasesAcc, ViewAcc}) ->
                                  {sets:union(Ids, sets:add_element(Id, Acc)),
                                   dict:erase(Id, ViewAcc)}
                          end, {Aliases, View}, Dead),
                    MemberSelf1 =
                        MemberSelf #view_member { left = From,
                                                  aliases = Aliases1 },
                    MemberLeft1 =
                        MemberLeft #view_member { right = Self },
                    store_view_members(
                      [MemberLeft1, MemberSelf1], View1)
            end
    end;
update_view(From, To, Self, View) ->
    %% The sender didn't realise we're on its right.
    case dict:fetch(Self, View) of
        #view_member { left = From, aliases = Aliases } ->
            %% No problem - it's either not received our birth yet; or
            %% it's not received notification of deaths of
            %% intermediates yet. Thus:
            true = ((dict:fetch(From, View)) #view_member.right =:= Self)
                orelse sets:is_element(To, Aliases), %% ASSERTION
            View;
        #view_member { left = Left, aliases = Aliases } = MemberSelf ->
            %% Neither us, or our new mystery Left, expected this to
            %% happen. Thus we need to detect deaths on the left and
            %% broadcast them.


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
