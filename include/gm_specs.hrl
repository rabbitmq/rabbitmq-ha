%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
%%

-include("gm.hrl").

-ifdef(use_specs).

-export_type([joined/0, members_changed/0, handle_msg/0, terminate/0]).

-type(callback_result() :: 'ok' | {'stop', any()}).

-type(joined()          :: #gm_joined          { args    :: [any()],
                                                 members :: [pid()] }).
-type(members_changed() :: #gm_members_changed { args    :: [any()],
                                                 births  :: [pid()],
                                                 deaths  :: [pid()] }).
-type(handle_msg()      :: #gm_handle_msg      { args    :: [any()],
                                                 from    :: pid(),
                                                 msg     :: any() }).
-type(terminate()       :: #gm_terminate       { args    :: [any()],
                                                 reason  :: term() }).

-spec(joined/1          :: (joined())          -> callback_result()).
-spec(members_changed/1 :: (members_changed()) -> callback_result()).
-spec(handle_msg/1      :: (handle_msg()])     -> callback_result()).
-spec(terminate/1       :: (terminate())       -> any()).

-endif.
