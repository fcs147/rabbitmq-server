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
         delete_non_existing_user/1,
         delete_existing_user/1,

         write_user_permission_for_non_existing_vhost/1,
         write_user_permission_for_non_existing_user/1,
         write_user_permission_for_existing_user/1
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
         delete_non_existing_user,
         delete_existing_user
        ]
       },
       {user_permissions, [],
        [
         write_user_permission_for_non_existing_vhost,
         write_user_permission_for_non_existing_user,
         write_user_permission_for_existing_user
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

write_non_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    %% Writing user in Mnesia works.
    mock_disabled_feature_flag(),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_mnesia(
         Username, User)),
    ?assertEqual(
       {ok, User},
       rabbit_auth_backend_internal:lookup_user_in_mnesia(Username)),

    %% Writing user in Khepri works. The return values MUST be idential to
    %% Mnesia!
    mock_enabled_feature_flag(),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_khepri(
         Username, User)),
    ?assertEqual(
       {ok, User},
       rabbit_auth_backend_internal:lookup_user_in_khepri(Username)),

    %% TODO: Check what is actually stored and where.
    %% TODO: Check that the other storage is not modified at the same time.

    ok.

write_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),
    DuplicateUser = internal_user:create_user(
                      Username, <<"other-password">>, undefined),

    %% Writing user twice in Mnesia is rejected. When we read the user again,
    %% we get the first version.
    mock_disabled_feature_flag(),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_mnesia(
         Username, User)),
    ?assertThrow(
       {error, {user_already_exists, Username}},
       rabbit_auth_backend_internal:add_user_sans_validation_in_mnesia(
         Username, DuplicateUser)),
    ?assertEqual(
       {ok, User},
       rabbit_auth_backend_internal:lookup_user_in_mnesia(Username)),

    %% Writing user twice in Khepri is rejected. When we read the user again,
    %% we get the first version. The return values MUST be idential to Mnesia!
    mock_enabled_feature_flag(),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_khepri(
         Username, User)),
    ?assertThrow(
       {error, {user_already_exists, Username}},
       rabbit_auth_backend_internal:add_user_sans_validation_in_khepri(
         Username, DuplicateUser)),
    ?assertEqual(
       {ok, User},
       rabbit_auth_backend_internal:lookup_user_in_khepri(Username)),

    ok.

delete_non_existing_user(_) ->
    Username = <<"alice">>,

    %% We first ensure the user doesn't exist in Mnesia, then we try to delete
    %% it.
    mock_disabled_feature_flag(),
    ?assertEqual(
       {error, not_found},
       rabbit_auth_backend_internal:lookup_user_in_mnesia(Username)),
    ?assertThrow(
       {error, {no_such_user, Username}},
       rabbit_auth_backend_internal:delete_user_in_mnesia(Username)),
    ?assertEqual(
       {error, not_found},
       rabbit_auth_backend_internal:lookup_user_in_mnesia(Username)),

    %% We first ensure the user doesn't exist in Khepri, then we try to delete
    %% it. The return values MUST be idential to Mnesia!
    mock_enabled_feature_flag(),
    ?assertEqual(
       {error, not_found},
       rabbit_auth_backend_internal:lookup_user_in_khepri(Username)),
    ?assertThrow(
       {error, {no_such_user, Username}},
       rabbit_auth_backend_internal:delete_user_in_khepri(Username)),
    ?assertEqual(
       {error, not_found},
       rabbit_auth_backend_internal:lookup_user_in_khepri(Username)),

    ok.

delete_existing_user(_) ->
    Username = <<"alice">>,
    User = internal_user:create_user(Username, <<"password">>, undefined),

    %% We write a user to Mnesia and delete it: it must exist before the
    %% deletion but not after.
    mock_disabled_feature_flag(),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_mnesia(
         Username, User)),
    ?assertEqual(
       {ok, User},
       rabbit_auth_backend_internal:lookup_user_in_mnesia(Username)),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:delete_user_in_mnesia(Username)),
    ?assertEqual(
       {error, not_found},
       rabbit_auth_backend_internal:lookup_user_in_mnesia(Username)),

    %% We write a user to Khepri and delete it: it must exist before the
    %% deletion but not after. The return values MUST be idential to Mnesia!
    mock_enabled_feature_flag(),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_khepri(
         Username, User)),
    ?assertEqual(
       {ok, User},
       rabbit_auth_backend_internal:lookup_user_in_khepri(Username)),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:delete_user_in_khepri(Username)),
    ?assertEqual(
       {error, not_found},
       rabbit_auth_backend_internal:lookup_user_in_khepri(Username)),

    ok.

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

    %% Writing user permissions in Mnesia works.
    mock_disabled_feature_flag(),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_mnesia(
         Username, User)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_mnesia(
         Username, VHostName)),
    ?assertThrow(
       {error, {no_such_vhost, VHostName}},
       rabbit_auth_backend_internal:set_permissions_in_mnesia(
         Username, VHostName, UserPermission)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_mnesia(
         Username, VHostName)),

    %% Writing user permissions in Khepri works.
    mock_enabled_feature_flag(),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_khepri(
         Username, User)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_khepri(
         Username, VHostName)),
    ?assertThrow(
       {error, {no_such_vhost, VHostName}},
       rabbit_auth_backend_internal:set_permissions_in_khepri(
         Username, VHostName, UserPermission)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_khepri(
         Username, VHostName)),

    ok.

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

    %% Writing user permissions in Mnesia works.
    mock_disabled_feature_flag(),
    ?assertEqual(
       VHost,
       rabbit_vhost:do_add_to_mnesia(VHostName, VHostDesc, VHostTags)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_mnesia(
         Username, VHostName)),
    ?assertThrow(
       {error, {no_such_user, Username}},
       rabbit_auth_backend_internal:set_permissions_in_mnesia(
         Username, VHostName, UserPermission)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_mnesia(
         Username, VHostName)),

    %% Writing user permissions in Khepri works.
    mock_enabled_feature_flag(),
    ?assertEqual(
       VHost,
       rabbit_vhost:do_add_to_khepri(VHostName, VHostDesc, VHostTags)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_khepri(
         Username, VHostName)),
    ?assertThrow(
       {error, {no_such_user, Username}},
       rabbit_auth_backend_internal:set_permissions_in_khepri(
         Username, VHostName, UserPermission)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_khepri(
         Username, VHostName)),

    ok.

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

    %% Writing user permissions in Mnesia works.
    mock_disabled_feature_flag(),
    ?assertEqual(
       VHost,
       rabbit_vhost:do_add_to_mnesia(VHostName, VHostDesc, VHostTags)),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_mnesia(
         Username, User)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_mnesia(
         Username, VHostName)),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:set_permissions_in_mnesia(
         Username, VHostName, UserPermission)),
    ?assert(
       rabbit_auth_backend_internal:check_vhost_access_in_mnesia(
         Username, VHostName)),

    %% Writing user permissions in Khepri works.
    mock_enabled_feature_flag(),
    ?assertEqual(
       VHost,
       rabbit_vhost:do_add_to_khepri(VHostName, VHostDesc, VHostTags)),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:add_user_sans_validation_in_khepri(
         Username, User)),
    ?assertNot(
       rabbit_auth_backend_internal:check_vhost_access_in_khepri(
         Username, VHostName)),
    ?assertEqual(
       ok,
       rabbit_auth_backend_internal:set_permissions_in_khepri(
         Username, VHostName, UserPermission)),
    ?assert(
       rabbit_auth_backend_internal:check_vhost_access_in_khepri(
         Username, VHostName)),

    ok.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

mock_enabled_feature_flag() -> mock_feature_flag_state(true).
mock_disabled_feature_flag() -> mock_feature_flag_state(false).

mock_feature_flag_state(State) ->
    meck:expect(rabbit_khepri, is_enabled, fun(_) -> State end).
