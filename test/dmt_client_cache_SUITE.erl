-module(dmt_client_cache_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% Common test callbacks
-export([
    all/0,
    groups/0,
    init_per_group/2,
    end_per_group/2
]).

%% Test cases
-export([
    cache_size_limits/1,
    cache_memory_limits/1,
    parallel_access/1,
    cache_invalidation/1
]).

-type config() :: ct_suite:ct_config().

%% CT callbacks

-spec all() -> [{group, atom()}].
all() ->
    [
        {group, cache_tests},
        {group, memory_limits}
    ].

-spec groups() -> [{atom(), list(), [atom()]}].
groups() ->
    [
        {cache_tests, [parallel], [
            cache_size_limits,
            parallel_access,
            cache_invalidation
        ]},
        % Sequential to avoid interference
        {memory_limits, [], [
            cache_memory_limits
        ]}
    ].

-spec init_per_group(atom(), config()) -> config().
init_per_group(cache_tests, Config) ->
    Apps = start_dmt_client(
        [
            {max_cache_size, #{
                elements => 2,
                % 50MB - large enough to not interfere
                memory => 52428800
            }}
        ]
    ),
    [{apps, Apps} | Config];
init_per_group(memory_limits, Config) ->
    Apps = start_dmt_client(
        [
            {max_cache_size, #{
                % Large enough to not interfere
                elements => 20,
                % 2KB - for testing memory limits
                memory => 2048
            }}
        ]
    ),
    [{apps, Apps} | Config].

-spec end_per_group(atom(), config()) -> any().
end_per_group(_Group, Config) ->
    stop_dmt_client(Config).

%% Tests

-spec cache_size_limits(config()) -> _.
cache_size_limits(_Config) ->
    % Create 3 objects when cache limit is 2
    {Ref1, RLOjb1, _} = make_test_object(make_domain_ref()),
    {Ref2, RLOjb2, _} = make_test_object(make_domain_ref()),
    {Ref3, RLOjb3, _} = make_test_object(make_domain_ref()),

    #domain_conf_v2_CommitResponse{version = GV1} =
        commit_insert(RLOjb1, Ref1),
    #domain_conf_v2_CommitResponse{version = GV2} =
        commit_insert(RLOjb2, Ref2),
    #domain_conf_v2_CommitResponse{version = GV3} =
        commit_insert(RLOjb3, Ref3),

    % Access all objects to ensure they're cached
    #domain_conf_v2_VersionedObject{} =
        dmt_client:checkout_object(Ref1, GV1),
    #domain_conf_v2_VersionedObject{} =
        dmt_client:checkout_object(Ref2, GV2),
    #domain_conf_v2_VersionedObject{} =
        dmt_client:checkout_object(Ref3, GV3),

    {error, object_not_found} = dmt_client_cache:do_get_object(Ref1, {version, GV1}),
    {ok, _} = dmt_client_cache:do_get_object(Ref2, {version, GV2}),
    {ok, _} = dmt_client_cache:do_get_object(Ref3, {version, GV3}).

-spec cache_memory_limits(config()) -> _.
cache_memory_limits(_Config) ->
    % Create object with large payload to hit memory limits (2KB limit set in init_per_suite)
    LargeDescription = create_large_binary(1300),

    % Create and cache two objects that together exceed memory limit
    {Ref1, RLObj1, Obj1} = make_test_object(make_domain_ref(), LargeDescription),
    {Ref2, RLObj2, Obj2} = make_test_object(make_domain_ref(), LargeDescription),

    % Insert and access objects
    #domain_conf_v2_CommitResponse{version = Version1} =
        commit_insert(RLObj1, Ref1),
    #domain_conf_v2_CommitResponse{version = Version2} =
        commit_insert(RLObj2, Ref2),

    % Access to ensure caching
    #domain_conf_v2_VersionedObject{object = Cached1} =
        dmt_client:checkout_object(Ref1, Version1),
    ?assertEqual(Obj1, Cached1),

    #domain_conf_v2_VersionedObject{object = Cached2} =
        dmt_client:checkout_object(Ref2, Version2),
    ?assertEqual(Obj2, Cached2),

    % First object should be evicted due to memory limits
    {error, object_not_found} = dmt_client_cache:do_get_object(Ref1, {version, Version1}),
    {ok, _} = dmt_client_cache:do_get_object(Ref2, {version, Version2}).

-spec parallel_access(config()) -> _.
parallel_access(_Config) ->
    {Ref, RLObject, Object} = make_test_object(make_domain_ref()),
    #domain_conf_v2_CommitResponse{version = Version} =
        commit_insert(RLObject, Ref),

    % Spawn multiple processes to access the same object
    Self = self(),
    Pids = [
        spawn_link(fun() ->
            #domain_conf_v2_VersionedObject{object = Result} =
                dmt_client:checkout_object(Ref, Version),
            Self ! {self(), Result}
        end)
     || _ <- lists:seq(1, 100)
    ],

    % Collect results
    Results = [
        receive
            {Pid, Result} -> Result
        end
     || Pid <- Pids
    ],

    % All results should be identical
    [?assertEqual(Object, Result) || Result <- Results].

-spec cache_invalidation(config()) -> _.
cache_invalidation(_Config) ->
    {Ref, RLObject, Object1} = make_test_object(make_domain_ref()),
    #domain_conf_v2_CommitResponse{version = Version1} =
        commit_insert(RLObject, Ref),

    % Access object to ensure it's cached
    #domain_conf_v2_VersionedObject{object = Result1} =
        dmt_client:checkout_object(Ref, Version1),
    ?assertEqual(Object1, Result1),

    % Update object
    {_, _, Object2} = make_test_object(Ref, <<"updated">>),
    #domain_conf_v2_CommitResponse{} =
        commit_update(Version1, Ref, Object2),

    % Check that we get updated version when requesting latest
    #domain_conf_v2_VersionedObject{object = Result2} =
        dmt_client:checkout_object(Ref, latest),
    ?assertEqual(Object2, Result2),

    % Original version should still be available and cached
    #domain_conf_v2_VersionedObject{object = Historical} =
        dmt_client:checkout_object(Ref, Version1),
    ?assertEqual(Object1, Historical).
%% Internal functions

make_domain_ref() ->
    {category, #domain_CategoryRef{id = rand:uniform(100000)}}.

make_test_object({category, #domain_CategoryRef{id = ID}} = Ref) ->
    Name = erlang:integer_to_binary(ID),
    make_test_object(Ref, Name).

make_test_object(
    {category, CategoryRef} = Ref,
    LargeDescription
) when
    is_binary(LargeDescription)
->
    Category = #domain_Category{
        name = <<"large_object">>,
        description = LargeDescription
    },
    ReflessObject = {category, Category},
    Object = {category, #domain_CategoryObject{ref = CategoryRef, data = Category}},
    {Ref, ReflessObject, Object}.

make_user_op_id() ->
    Params = #domain_conf_v2_UserOpParams{email = genlib:unique(), name = genlib:unique()},
    #domain_conf_v2_UserOp{id = ID} =
        dmt_client_user_op:create(Params, #{}),
    ID.

commit_insert(Object, Reference) ->
    Op = {insert, #domain_conf_v2_InsertOp{object = Object, force_ref = Reference}},
    Commit = #domain_conf_v2_Commit{ops = [Op]},
    UserOpID = make_user_op_id(),
    dmt_client:commit(1, Commit, UserOpID).

commit_update(Version, Ref, Object) ->
    Op =
        {update, #domain_conf_v2_UpdateOp{
            targeted_ref = Ref,
            new_object = Object
        }},
    Commit = #domain_conf_v2_Commit{ops = [Op]},
    UserOpID = make_user_op_id(),
    dmt_client:commit(Version, Commit, UserOpID).

create_large_binary(Size) ->
    list_to_binary(lists:duplicate(Size, $a)).

% insert_and_access(Object) ->
%     Version = commit_insert(Object),
%     VersionRef = {version, Version},
%     {Ref, _, _} = Object,
%     % Access to cache it
%     #domain_conf_v2_VersionedObject{} = dmt_client:checkout_object(VersionRef, Ref),
%     {Ref, VersionRef}.

-spec start_dmt_client(list()) -> [atom()].
start_dmt_client(ExtraConfig) ->
    Config =
        ExtraConfig ++
            [
                {service_urls, #{
                    'Repository' => <<"http://dmt:8022/v1/domain/repository">>,
                    'RepositoryClient' => <<"http://dmt:8022/v1/domain/repository_client">>,
                    'UserOpManagement' => <<"http://dmt:8022/v1/domain/user_op">>
                }}
            ],
    genlib_app:start_application_with(dmt_client, Config).

-spec stop_dmt_client(config()) -> ok.
stop_dmt_client(Config) ->
    genlib_app:stop_unload_applications(?config(apps, Config)).
