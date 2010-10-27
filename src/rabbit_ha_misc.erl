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

-module(rabbit_ha_misc).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([ha_nodes_or_die/1, ha_nodes/1, filter_out_ha_node/2, create_queue/2,
         foldl_rpc/4, ensure_ha_queues/2]).

-define(HA_NODES_KEY, <<"ha-nodes">>).

ha_nodes_or_die(#exchange { arguments = Args }) ->
    case rabbit_misc:table_lookup(Args, ?HA_NODES_KEY) of
        undefined ->
            rabbit_misc:protocol_error(
              precondition_failed,
              "No ~s argument in exchange declaration",
              [binary_to_list(?HA_NODES_KEY)]);
        {array, []} ->
            rabbit_misc:protocol_error(
              precondition_failed,
              "An HA exchange needs at least one initial node", []);
        _ ->
            ha_nodes(Args)
    end.

ha_nodes(Args) ->
    {array, Array} = rabbit_misc:table_lookup(Args, ?HA_NODES_KEY),
    [maybe_binary_to_atom(Node) || {longstr, Node} <- Array].

filter_out_ha_node(Node, Args) when is_atom(Node) ->
    filter_out_ha_node(atom_to_binary(Node, utf8), Args);
filter_out_ha_node(Node, Args) when is_binary(Node) ->
    {array, Array} = rabbit_misc:table_lookup(Args, ?HA_NODES_KEY),
    Array1 = [Elem || {longstr, Node1} = Elem <- Array, Node /= Node1],
    lists:keyreplace(?HA_NODES_KEY, 1, Args, {?HA_NODES_KEY, array, Array1}).

maybe_binary_to_atom(Binary) when is_binary(Binary) ->
    case get({binary_as_atom, Binary}) of
        undefined ->
            Atom = binary_to_atom(Binary, utf8),
            undefined = put({binary_as_atom, Binary}, Atom),
            Atom;
        Atom when is_atom(Atom) ->
            Atom
    end.

create_queue(X, QDeclArgs) ->
    HAQPid = case rabbit_ha_sup:start_child([X]) of
                 {ok, Pid}                       -> Pid;
                 {error, {already_started, Pid}} -> Pid
             end,
    ok = case apply(rabbit_amqqueue, declare, QDeclArgs) of
             {new, Q}       -> rabbit_ha_queue:new_producer_queue(HAQPid, Q);
             {existing, _Q} -> ok
         end.

ensure_ha_queues(X, Nodes) ->
    ok = foldl_rpc(
           fun (Node, ok, KeysAcc) ->
                   {ok, [rpc:async_call(Node, rabbit_ha_sup, start_child, [[X]])
                         | KeysAcc]}
           end, ok, Nodes,
           fun ({ok, _Pid})                       -> ok;
               ({error, {already_started, _Pid}}) -> ok
           end).

foldl_rpc(Fun, Init, List, ValidateFun) ->
    {Result, Keys} = lists:foldl(
                       fun (Elem, {Acc, KeysAcc}) ->
                               Fun(Elem, Acc, KeysAcc)
                       end, {Init, []}, List),
    ok =
        lists:foldl(fun (Key, ok) -> ValidateFun(rpc:yield(Key)) end, ok, Keys),
    Result.
