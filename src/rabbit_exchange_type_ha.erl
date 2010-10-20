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

-module(rabbit_exchange_type_ha).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_exchange_type_spec.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, route/2]).
-export([validate/1, create/1, recover/2, delete/2, add_binding/2,
         remove_bindings/2, assert_args_equivalence/2]).

-define(HA_NODES_KEY, <<"ha-nodes">>).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type ha"},
                    {mfa,         {rabbit_exchange_type_registry, register,
                                   [<<"x-ha">>, ?MODULE]}},
                    {requires,    rabbit_exchange_type_registry},
                    {enables,     kernel_ready}]}).

description() ->
    [{name, <<"x-ha">>},
     {description, <<"HA exchange type">>}].

route(#exchange{ name = XName, arguments = Args }, _Delivery) ->
    Me = self(),
    Nodes = node_array_to_nodes(rabbit_misc:table_lookup(Args, ?HA_NODES_KEY)),
    ensure_queues(
      [{rabbit_misc:r(XName, queue, << (XName#resource.name)/binary,
                                       (term_to_binary(Node))/binary,
                                       (term_to_binary(Me))/binary >>),
        Node} || Node <- Nodes]).

validate(#exchange { arguments = Args }) ->
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
        AmqpArray ->
            Nodes = node_array_to_nodes(AmqpArray),
            case Nodes -- mnesia:system_info(running_db_nodes) of
                [] ->
                    ok;
                NotRunning ->
                    rabbit_misc:protocol_error(
                      precondition_failed,
                      "Nodes indicated in arguments, but not running: ~w",
                      NotRunning)
            end
    end.

create(#exchange { arguments = Args } = X) ->
    ok = ensure_ha_queues(X, node_array_to_nodes(
                               rabbit_misc:table_lookup(Args, ?HA_NODES_KEY))).

recover(_X, []) -> ok.

delete(_X, []) -> ok.

add_binding(_X, _B) ->
    rabbit_misc:protocol_error(
      not_allowed,
      "cannot create bindings with an HA exchange as the binding source", []).

remove_bindings(_X, _Bs) ->
    rabbit_misc:protocol_error(
      not_allowed,
      "cannot create bindings with an HA exchange as the binding source", []).

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%%----------------------------------------------------------------------------

node_array_to_nodes({array, Array}) when is_list(Array) ->
    [maybe_binary_to_atom(Node) || {longstr, Node} <- Array].

maybe_binary_to_atom(Binary) when is_binary(Binary) ->
    case get({binary_as_atom, Binary}) of
        undefined ->
            Atom = binary_to_atom(Binary, utf8),
            undefined = put({binary_as_atom, Binary}, Atom),
            Atom;
        Atom when is_atom(Atom) ->
            Atom
    end.

ensure_queues(QNameNodes) ->
    foldl_rpc(
      fun ({QName, Node}, QNamesAcc, KeysAcc) ->
              {[QName | QNamesAcc],
               case rabbit_amqqueue:lookup(QName) of
                   {ok, #amqqueue{}} ->
                       KeysAcc;
                   {error, not_found} ->
                       [rpc:async_call(
                          Node, rabbit_amqqueue, declare,
                          [QName, false, true, [], none]) | KeysAcc]
               end}
      end, [], QNameNodes,
      fun ({new, #amqqueue {}}) -> ok end).

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
