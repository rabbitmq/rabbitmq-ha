-module(gm_test).

-compile([export_all]).

get_state() ->
    case get(?MODULE) of
        undefined ->
            dict:new();
        Dict ->
            Dict
    end.

with_state(Fun) ->
    put(?MODULE, Fun(get_state())).

callback(From, Msg) ->
    with_state(
      fun (State) ->
              case Msg of
                  {member_joined, Member} ->
                      false = dict:is_key(Member, State),
                      io:format("~p: + ~p~n", [self(), Member]),
                      State;
                  {member_left, Member} ->
                      io:format("~p: - ~p (~p)~n",
                                [self(), Member, dict:find(Member, State)]),
                      dict:erase(Member, State);
                  {test_msg, Num} ->
                      ok = case dict:find(From, State) of
                               {ok, empty} -> ok;
                               {ok, Num}   -> ok;
                               error       -> ok
                           end,
                      dict:store(From, Num + 1, State)
              end
      end).

spawn_member() ->
    spawn_link(
      fun () ->
              random:seed(now()),
              %% start up delay of no more than 10 seconds
              timer:sleep(random:uniform(10000)),
              {ok, Pid} = gm:join(?MODULE, fun callback/2),
              io:format("Joined ~p~n", [Pid]),
              Start = random:uniform(10000),
              send_loop(Pid, Start, Start + random:uniform(10000)),
              gm:leave(Pid),
              io:format("Left ~p~n", [Pid]),
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
