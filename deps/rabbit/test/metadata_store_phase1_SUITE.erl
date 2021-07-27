%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(metadata_store_phase1_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("khepri/include/khepri.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2,

         write_non_existing_user/1,
         write_existing_user/1,
         update_non_existing_user/1,
         update_existing_user/1,
         delete_non_existing_user/1,
         delete_existing_user/1,

         write_user_permission_for_non_existing_vhost/1,
         write_user_permission_for_non_existing_user/1,
         write_user_permission_for_existing_user/1,
         check_resource_access/1,
         clear_user_permission/1,
         delete_user_and_check_resource_access/1,

         write_topic_permission_for_non_existing_vhost/1,
         write_topic_permission_for_non_existing_user/1,
         write_topic_permission_for_existing_user/1,
         clear_specific_topic_permission/1,
         clear_all_topic_permissions/1,
         delete_user_and_check_topic_access/1
        ]).

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
     {group, internal_users}
    ].

groups() ->
    [
     {internal_users, [],
      [
       {users, [],
        [
         write_non_existing_user,
         write_existing_user,
         update_non_existing_user,
         update_existing_user,
         delete_non_existing_user,
         delete_existing_user
        ]
       },
       {user_permissions, [],
        [
         write_user_permission_for_non_existing_vhost,
         write_user_permission_for_non_existing_user,
         write_user_permission_for_existing_user,
         check_resource_access,
         clear_user_permission,
         delete_user_and_check_resource_access
        ]
       },
       {topic_permissions, [],
        [
         write_topic_permission_for_non_existing_vhost,
         write_topic_permission_for_non_existing_user,
         write_topic_permission_for_existing_user,
         clear_specific_topic_permission,
         clear_all_topic_permissions,
         delete_user_and_check_topic_access
        ]
       }
      ]
     }
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    %%rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [
       fun init_feature_flags/1,
       fun setup_mnesia/1,
       fun setup_khepri/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

setup_mnesia(Config) ->
    %% Configure Mnesia directory in the common_test priv_dir and start it.
    MnesiaDir = filename:join(
                  ?config(priv_dir, Config),
                  "mnesia"),
    ct:pal("Mnesia directory: ~ts", [MnesiaDir]),
    ok = file:make_dir(MnesiaDir),
    ok = application:load(mnesia),
    ok = application:set_env(mnesia, dir, MnesiaDir),
    ok = mnesia:create_schema([node()]),
    {ok, _} = application:ensure_all_started(mnesia),

    %% Bypass rabbit_misc:execute_mnesia_transaction/1 (no worker_pool
    %% configured in particular) but keep the behavior of throwing the error.
    meck:expect(
      rabbit_misc, execute_mnesia_transaction,
      fun(Fun) ->
              case mnesia:sync_transaction(Fun) of
                  {atomic, Result}  -> Result;
                  {aborted, Reason} -> throw({error, Reason})
              end
      end),

    ct:pal("Mnesia info below:"),
    mnesia:info(),
    Config.

setup_khepri(Config) ->
    %% Start Khepri.
    {ok, _} = application:ensure_all_started(khepri),

    %% Configure Khepri. It takes care of configuring Ra system & cluster. It
    %% uses the Mnesia directory to store files.
    ok = rabbit_khepri:setup(undefined),

    ct:pal("Khepri info below:"),
    rabbit_khepri:info(),
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),

    %% Create Mnesia tables.
    TableDefs = rabbit_table:pre_khepri_definitions(),
    lists:foreach(
      fun ({Table, Def}) -> ok = rabbit_table:create(Table, Def) end,
      TableDefs),

    Config.

end_per_testcase(Testcase, Config) ->
    %% Delete Mnesia tables to clear any data.
    TableDefs = rabbit_table:pre_khepri_definitions(),
    lists:foreach(
      fun ({Table, _}) -> {atomic, ok} = mnesia:delete_table(Table) end,
      TableDefs),

    %% Clear all data in Khepri.
    ok = rabbit_khepri:clear_store(),

    rabbit_ct_helpers:testcase_finished(Config, Testcase).

init_feature_flags(Config) ->
    FFFile = filename:join(
                  ?config(priv_dir, Config),
                  "feature_flags"),
    ct:pal("Feature flags file: ~ts", [FFFile]),
    ok = application:load(rabbit),
    ok = application:set_env(rabbit, feature_flags_file, FFFile),
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

-define(with(T), fun(With) -> T end).

%%
%% Users
%%

write_non_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    %% TODO: Check what is actually stored and where.
    %% TODO: Check that the other storage is not modified at the same time.

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              {ok, User},
              lookup_user(With, Username)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

write_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertThrow(
              {error, {user_already_exists, Username}},
              add_user(With, Username, User))),
     ?with(?assertEqual(
              {ok, User},
              lookup_user(With, Username)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

update_non_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UpdatedUser = internal_user:set_password_hash(
                    User, <<"updated-pw">>, undefined),
    Fun = fun(_) -> UpdatedUser end,

    Tests =
    [
     ?with(?assertEqual(
              {error, not_found},
              lookup_user(With, Username))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              update_user(With, Username, Fun))),
     ?with(?assertEqual(
              {error, not_found},
              lookup_user(With, Username)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

update_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UpdatedUser = internal_user:set_password_hash(
                    User, <<"updated-pw">>, undefined),
    Fun = fun(_) -> UpdatedUser end,

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              {ok, User},
              lookup_user(With, Username))),
     ?with(?assertEqual(
              ok,
              update_user(With, Username, Fun))),
     ?with(?assertEqual(
              {ok, UpdatedUser},
              lookup_user(With, Username)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

delete_non_existing_user(_) ->
    Username = <<"alice">>,

    Tests =
    [
     ?with(?assertEqual(
              {error, not_found},
              lookup_user(With, Username))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              delete_user(With, Username))),
     ?with(?assertEqual(
              {error, not_found},
              lookup_user(With, Username)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

delete_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              {ok, User},
              lookup_user(With, Username))),
     ?with(?assertEqual(
              ok,
              delete_user(With, Username))),
     ?with(?assertEqual(
              {error, not_found},
              lookup_user(With, Username)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

%%
%% User permissions
%%

write_user_permission_for_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<>>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertNot(
              check_vhost_access(With, Username, VHostName))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              set_permissions(With, Username, VHostName, UserPermission))),
     ?with(?assertNot(
              check_vhost_access(With, Username, VHostName)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

write_user_permission_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<>>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertNot(
              check_vhost_access(With, Username, VHostName))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              set_permissions(With, Username, VHostName, UserPermission))),
     ?with(?assertNot(
              check_vhost_access(With, Username, VHostName)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

write_user_permission_for_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<>>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertNot(
              check_vhost_access(With, Username, VHostName))),
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertNot(
              check_vhost_access(With, Username, VHostName))),
     ?with(?assertEqual(
              ok,
              set_permissions(With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_vhost_access(With, Username, VHostName)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

check_resource_access(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<"my-resource">>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_permissions(With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_resource_access(
                With, Username, VHostName, "my-resource", configure))),
     ?with(?assertNot(
              check_resource_access(
                With, Username, VHostName, "my-resource", write))),
     ?with(?assertNot(
              check_resource_access(
                With, Username, VHostName, "other-resource", configure)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

clear_user_permission(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<"my-resource">>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_permissions(With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_resource_access(
                With, Username, VHostName, "my-resource", configure))),
     ?with(?assertEqual(
              ok,
              clear_permissions(With, Username, VHostName))),
     ?with(?assertNot(
              check_resource_access(
                With, Username, VHostName, "my-resource", configure)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

delete_user_and_check_resource_access(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VHostName},
                        permission = #permission{
                                        configure  = <<"my-resource">>,
                                        write      = <<>>,
                                        read       = <<>>}},

    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_permissions(With, Username, VHostName, UserPermission))),
     ?with(?assert(
              check_vhost_access(With, Username, VHostName))),
     ?with(?assert(
              check_resource_access(
                With, Username, VHostName, "my-resource", configure))),
     ?with(?assertEqual(
              ok,
              delete_user(With, Username))),
     ?with(?assertNot(
              check_vhost_access(With, Username, VHostName))),
     ?with(?assertNot(
              check_resource_access(
                With, Username, VHostName, "my-resource", configure)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

%%
%% Topic permissions
%%

write_topic_permission_for_non_existing_vhost(_) ->
    VHostName = <<"vhost">>,
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<>>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    %% Unset permissions equals to permissions granted.
    Tests =
    [
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_vhost, VHostName}},
              set_topic_permissions(
                With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

write_topic_permission_for_non_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<>>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertThrow(
              {error, {no_such_user, Username}},
              set_topic_permissions(
                With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context)))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

write_topic_permission_for_existing_user(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<"^key$">>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>})))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

clear_specific_topic_permission(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<"^key$">>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    %% TODO: Add another topic permission to verify it's still present after
    %% clear_topic_permissions().
    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              ok,
              clear_topic_permissions(With, Username, VHostName, Exchange))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>})))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

clear_all_topic_permissions(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<"^key$">>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              ok,
              clear_topic_permissions(With, Username, VHostName))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>})))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

delete_user_and_check_topic_access(_) ->
    VHostName = <<"vhost">>,
    VHostDesc = <<>>,
    VHostTags = [],
    VHost = vhost_v1:new(VHostName, VHostTags),
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    Exchange = <<"exchange">>,
    TopicPermission = #topic_permission{
                         topic_permission_key =
                         #topic_permission_key{
                            user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostName},
                            exchange = Exchange},
                         permission = #permission{
                                         write = <<>>,
                                         read = <<"^key$">>}
                        },
    Context = #{routing_key => <<"key">>,
                variable_map => #{<<"vhost">> => VHostName,
                                  <<"username">> => Username}},

    Tests =
    [
     ?with(?assertEqual(
              VHost,
              add_vhost(With, VHostName, VHostDesc, VHostTags))),
     ?with(?assertEqual(
              ok,
              add_user(With, Username, User))),
     ?with(?assertEqual(
              ok,
              set_topic_permissions(
                With, Username, VHostName, Exchange, TopicPermission))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assertNot(
              check_topic_access(
                With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>}))),
     ?with(?assertEqual(
              ok,
              delete_user(With, Username))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read, Context))),
     ?with(?assert(
              check_topic_access(
                With, Username, VHostName, Exchange, read,
                Context#{routing_key => <<"something-else">>})))
    ],

    ?assertEqual(
       ok,
       eunit:test(
         [{setup, fun mock_disabled_feature_flag/0, [{with, mnesia, Tests}]},
          {setup, fun mock_enabled_feature_flag/0,  [{with, khepri, Tests}]}],
         [verbose])).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

mock_enabled_feature_flag() -> mock_feature_flag_state(true).
mock_disabled_feature_flag() -> mock_feature_flag_state(false).

mock_feature_flag_state(State) ->
    meck:expect(rabbit_khepri, is_enabled, fun(_) -> State end).

add_vhost(mnesia, VHostName, VHostDesc, VHostTags) ->
    rabbit_vhost:do_add_to_mnesia(VHostName, VHostDesc, VHostTags);
add_vhost(khepri, VHostName, VHostDesc, VHostTags) ->
    rabbit_vhost:do_add_to_khepri(VHostName, VHostDesc, VHostTags).

add_user(mnesia, Username, User) ->
    rabbit_auth_backend_internal:add_user_sans_validation_in_mnesia(
      Username, User);
add_user(khepri, Username, User) ->
    rabbit_auth_backend_internal:add_user_sans_validation_in_khepri(
      Username, User).

lookup_user(mnesia, Username) ->
    rabbit_auth_backend_internal:lookup_user_in_mnesia(Username);
lookup_user(khepri, Username) ->
    rabbit_auth_backend_internal:lookup_user_in_khepri(Username).

update_user(mnesia, Username, Fun) ->
    rabbit_auth_backend_internal:update_user_in_mnesia(Username, Fun);
update_user(khepri, Username, Fun) ->
    rabbit_auth_backend_internal:update_user_in_khepri(Username, Fun).

delete_user(mnesia, Username) ->
    rabbit_auth_backend_internal:delete_user_in_mnesia(Username);
delete_user(khepri, Username) ->
    rabbit_auth_backend_internal:delete_user_in_khepri(Username).

set_permissions(mnesia, Username, VHostName, UserPermission) ->
    rabbit_auth_backend_internal:set_permissions_in_mnesia(
      Username, VHostName, UserPermission);
set_permissions(khepri, Username, VHostName, UserPermission) ->
    rabbit_auth_backend_internal:set_permissions_in_khepri(
      Username, VHostName, UserPermission).

check_vhost_access(mnesia, Username, VHostName) ->
    rabbit_auth_backend_internal:check_vhost_access_in_mnesia(
      Username, VHostName);
check_vhost_access(khepri, Username, VHostName) ->
    rabbit_auth_backend_internal:check_vhost_access_in_khepri(
      Username, VHostName).

set_topic_permissions(mnesia, Username, VHostName, Exchange, TopicPermission) ->
    rabbit_auth_backend_internal:set_topic_permissions_in_mnesia(
      Username, VHostName, Exchange, TopicPermission);
set_topic_permissions(khepri, Username, VHostName, Exchange, TopicPermission) ->
    rabbit_auth_backend_internal:set_topic_permissions_in_khepri(
      Username, VHostName, Exchange, TopicPermission).

check_topic_access(mnesia, Username, VHostName, Exchange, Perm, Context) ->
    rabbit_auth_backend_internal:check_topic_access_in_mnesia(
      Username, VHostName, Exchange, Perm, Context);
check_topic_access(khepri, Username, VHostName, Exchange, Perm, Context) ->
    rabbit_auth_backend_internal:check_topic_access_in_khepri(
      Username, VHostName, Exchange, Perm, Context).

clear_permissions(mnesia, Username, VHostName) ->
    rabbit_auth_backend_internal:clear_permissions_in_mnesia(
      Username, VHostName);
clear_permissions(khepri, Username, VHostName) ->
    rabbit_auth_backend_internal:clear_permissions_in_khepri(
      Username, VHostName).

check_resource_access(mnesia, Username, VHostName, Resource, Perm) ->
    rabbit_auth_backend_internal:check_resource_access_in_mnesia(
      Username, VHostName, Resource, Perm);
check_resource_access(khepri, Username, VHostName, Resource, Perm) ->
    rabbit_auth_backend_internal:check_resource_access_in_khepri(
      Username, VHostName, Resource, Perm).

clear_topic_permissions(mnesia, Username, VHostName) ->
    rabbit_auth_backend_internal:clear_topic_permissions_in_mnesia(
      Username, VHostName);
clear_topic_permissions(khepri, Username, VHostName) ->
    rabbit_auth_backend_internal:clear_topic_permissions_in_khepri(
      Username, VHostName).

clear_topic_permissions(mnesia, Username, VHostName, Exchange) ->
    rabbit_auth_backend_internal:clear_topic_permissions_in_mnesia(
      Username, VHostName, Exchange);
clear_topic_permissions(khepri, Username, VHostName, Exchange) ->
    rabbit_auth_backend_internal:clear_topic_permissions_in_khepri(
      Username, VHostName, Exchange).
