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

-module(rabbit_ha_queue).

-behaviour(gen_server2).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/1]).
-export([link_to_neighbours/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3,
         prioritise_call/3, prioritise_cast/2, prioritise_info/2]).

-record(state, { name,
                 upstream,
                 downstream,
                 server
               }).

start_link(X = #exchange { name = XName }) ->
    Server = binary_to_atom(XName #resource.name, utf8),
    gen_server2:start_link({local, Server}, ?MODULE, [X, Server], []).

link_to_neighbours(Pid) ->
    gen_server2:cast(Pid, link_to_neighbours).

init([#exchange{ name = XName }, Server]) ->
    ok = link_to_neighbours(self()),
    {ok, #state { name       = XName,
                  upstream   = undefined,
                  downstream = undefined,
                  server     = Server }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(link_to_neighbours, State = #state { name = Name }) ->
    {ok, #exchange { arguments = Args }} = rabbit_exchange:lookup(Name),
    link_neighbours_or_stop(Args, State).

handle_info({'DOWN', MRef, process, _Pid, _Reason},
            State = #state { upstream   = Upstream,
                             downstream = Downstream,
                             server     = Server}) ->
    case detect_neighbour_death(MRef, [Upstream, Downstream]) of
        not_found -> {noreply, State};
        Node      -> Args = remove_node_from_exchange_args(Node, State),
                     %% after removing from mnesia, make sure the dead
                     %% node knows about it (it may have immediately
                     %% come back up and not noticed it died)
                     ok = link_to_neighbours({Server, Node}),
                     link_neighbours_or_stop(Args, State)
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

prioritise_call(_Msg, _From, _State) -> 0.

prioritise_cast(link_to_neighbours, _State) -> 8;
prioritise_cast(_Msg,               _State) -> 0.

prioritise_info({'DOWN', _MonitorRef, process, _Pid, _Reason}, _State) -> 8;
prioritise_info(_Msg,                                          _State) -> 0.

detect_neighbour_death(_MRef, []) ->
    not_found;
detect_neighbour_death(MRef, [{Node, MRef} | _]) ->
    Node;
detect_neighbour_death(MRef, [_ | Rest]) ->
    detect_neighbour_death(MRef, Rest).

remove_node_from_exchange_args(DeadNode, #state { name = Name }) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              [#exchange { arguments = Args } = Exchange] =
                  mnesia:read(rabbit_exchange, Name, write),
              case rabbit_ha_misc:filter_out_ha_node(DeadNode, Args) of
                  Args -> %% nothing changed
                      Args;
                  Args1 ->
                      mnesia:write(rabbit_exchange,
                                   Exchange #exchange { arguments = Args1 },
                                   write),
                      Args1
              end
      end).

link_neighbours_or_stop(Args, State) ->
    case link_neighbours(Args, State) of
        {not_found, State1} ->
            {stop, normal, State1};
        {ok, State1} ->
            {noreply, State1}
    end.    

link_neighbours(Args, State) ->
    Nodes = rabbit_ha_misc:ha_nodes(Args),
    case find_neighbours(node(), Nodes) of
        not_found ->
            {not_found, State};
        {Up, Down} ->
            Upstream = contact_neighbour(Up, State #state.upstream, State),
            Downstream =
                contact_neighbour(Down, State #state.downstream, State),
            {ok, State #state { upstream   = Upstream,
                                downstream = Downstream }}
    end.

find_neighbours(_Me, []) ->
    not_found;
find_neighbours(Me, [Me]) ->
    {undefined, undefined};
find_neighbours(Me, [Me, Downstream | _]) ->
    {undefined, Downstream};
find_neighbours(Me, [Upstream, Me]) ->
    {Upstream, undefined};
find_neighbours(Me, [Upstream, Me, Downstream]) ->
    {Upstream, Downstream};
find_neighbours(Me, [_ | Rest]) ->
    find_neighbours(Me, Rest).

contact_neighbour(undefined, undefined, _State) ->
    undefined;
contact_neighbour(Neighbour, {Neighbour, _MRef} = Result, _State) ->
    Result;
contact_neighbour(Neighbour, undefined, #state { server = Server }) ->
    MRef = erlang:monitor(process, {Server, Neighbour}),
    ok = link_to_neighbours({Server, Neighbour}),
    {Neighbour, MRef};
contact_neighbour(Neighbour, {OldNeighbour, MRef},
                  State = #state { server = Server }) ->
    true = erlang:demonitor(MRef),
    ok = link_to_neighbours({Server, OldNeighbour}),
    contact_neighbour(Neighbour, undefined, State).

is_master(#state { upstream = undefined }) -> true;
is_master(_State)                          -> false.

is_slave(State) -> not is_master(State).
