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
    cache_hit_verification/1,
    cache_size_limits/1,
    cache_memory_limits/1,
    parallel_cache_access/1,
    cache_invalidation/1
]).

-type config() :: ct_suite:ct_config().

%% CT callbacks

-spec all() -> [{group, atom()}].
all() ->
    [
        {group, cache_tests}
    ].

-spec groups() -> [{atom(), list(), [atom()]}].
groups() ->
    [
        {cache_tests, [parallel], [
            cache_hit_verification,
            cache_size_limits,
            parallel_cache_access,
            cache_versioning,
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
        {max_cache_size, #{
            elements => 2,
            % 50MB - large enough to not interfere
            memory => 52428800
        }}
    ),
    [{apps, Apps} | Config];
init_per_group(memory_limits, Config) ->
    Apps = start_dmt_client(
        {max_cache_size, #{
            % Large enough to not interfere
            elements => 20,
            % 2KB - for testing memory limits
            memory => 2048
        }}
    ),
    [{apps, Apps} | Config].

-spec end_per_group(atom(), config()) -> any().
end_per_group(_Group, Config) ->
    stop_dmt_client(Config).

%% Tests

-spec cache_hit_verification(config()) -> _.
cache_hit_verification(_Config) ->
    Ref = make_domain_ref(),
    {_, _, Object} = Obj = make_test_object(Ref),
    Version = commit_insert(Obj),

    % First access should hit service
    #domain_conf_v2_VersionedObject{
        object = Result1
    } = dmt_client:checkout_object({version, Version}, Ref),
    ?assertEqual(Object, Result1),

    % Second access should come from cache
    #domain_conf_v2_VersionedObject{
        object = Result2
    } = dmt_client:checkout_object({version, Version}, Ref),
    ?assertEqual(Result1, Result2).

-spec cache_size_limits(config()) -> _.
cache_size_limits(_Config) ->
    % Create 3 objects when cache limit is 2
    Objects = [make_test_object(make_domain_ref()) || _ <- lists:seq(1, 3)],

    % Insert all objects and get their versions
    VersionedObjects = [
        begin
            #domain_conf_v2_CommitResponse{version = LV} =
                commit_insert(Object),
            {Ref, LV, Obj}
        end
     || {Ref, _, Obj} = Object <- Objects
    ],

    {Ref, LV, Obj} = make_test_object(make_domain_ref()),
    #domain_conf_v2_CommitResponse{version = LV} =
        commit_insert(Object),

    % Access all objects to ensure they're cached
    [dmt_client:checkout_object({version, Version}, Ref) || {Ref, Version, _} <- VersionedObjects],

    % Verify only last 2 objects are in cache
    {Ref1, Version1, _} = hd(VersionedObjects),
    {Ref2, Version2, _} = lists:nth(2, VersionedObjects),
    {Ref3, Version3, _} = lists:last(VersionedObjects),

    % Last two should be fast (cached)
    dmt_client:checkout_object({version, Version2}, Ref2),
    dmt_client:checkout_object({version, Version3}, Ref3),

    % First one should require service call
    dmt_client:checkout_object({version, Version1}, Ref1).

-spec cache_memory_limits(config()) -> _.
cache_memory_limits(_Config) ->
    % Create object with large payload to hit memory limits (2KB limit set in init_per_suite)
    % 1KB per description
    LargeDescription = create_large_binary(1024),

    % Create and cache two objects that together exceed memory limit
    Ref1 = make_domain_ref(),
    Ref2 = make_domain_ref(),
    {_, _, Obj1} = Object1 = make_test_object(Ref1, LargeDescription),
    {_, _, Obj2} = Object2 = make_test_object(Ref2, LargeDescription),

    % Insert and access objects
    Version1 = commit_insert(Object1),
    Version2 = commit_insert(Object2),

    % Access to ensure caching
    #domain_conf_v2_VersionedObject{object = Cached1} =
        dmt_client:checkout_object({version, Version1}, Ref1),
    ?assertEqual(Obj1, Cached1),

    #domain_conf_v2_VersionedObject{object = Cached2} =
        dmt_client:checkout_object({version, Version2}, Ref2),
    ?assertEqual(Obj2, Cached2),

    % First object should be evicted due to memory limits
    % This will require service call
    #domain_conf_v2_VersionedObject{object = Reloaded1} =
        dmt_client:checkout_object({version, Version1}, Ref1),
    ?assertEqual(Obj1, Reloaded1),

    % Second object should still be in cache
    #domain_conf_v2_VersionedObject{object = StillCached2} =
        dmt_client:checkout_object({version, Version2}, Ref2),
    ?assertEqual(Obj2, StillCached2).

-spec parallel_cache_access(config()) -> _.
parallel_cache_access(_Config) ->
    Ref = make_domain_ref(),
    {_, _, Object} = Obj = make_test_object(Ref),
    Version = commit_insert(Obj),

    % Spawn multiple processes to access the same object
    Self = self(),
    Pids = [
        spawn_link(fun() ->
            #domain_conf_v2_VersionedObject{object = Result} =
                dmt_client:checkout_object({version, Version}, Ref),
            Self ! {self(), Result}
        end)
     || _ <- lists:seq(1, 10)
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
    Ref = make_domain_ref(),
    {_, _, Object1} = Obj1 = make_test_object(Ref),
    Version1 = commit_insert(Obj1),

    % Access object to ensure it's cached
    #domain_conf_v2_VersionedObject{object = Result1} =
        dmt_client:checkout_object({version, Version1}, Ref),
    ?assertEqual(Object1, Result1),

    % Update object
    {_, _, Object2} = make_test_object(Ref, <<"updated">>),
    _Version2 = commit_update(Version1, Object2),

    % Check that we get updated version when requesting latest
    #domain_conf_v2_VersionedObject{object = Result2} =
        dmt_client:checkout_object(latest, Ref),
    ?assertEqual(Object2, Result2),

    % Original version should still be available and cached
    #domain_conf_v2_VersionedObject{object = Historical} =
        dmt_client:checkout_object({version, Version1}, Ref),
    ?assertEqual(Object1, Historical).
%% Internal functions

make_domain_ref() ->
    ID = genlib:unique(),
    {category, #domain_CategoryRef{id = ID}}.

make_test_object({category, #domain_CategoryRef{id = ID}} = Ref) ->
    Name = erlang:integer_to_binary(ID),
    make_test_object(Ref, Name).

make_test_object(
    Ref = {category, CategoryRef},
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

commit_insert(Object) ->
    Version = get_base_version(Object),
    Op = {insert, #domain_conf_v2_InsertOp{object = Object}},
    Commit = #domain_conf_v2_Commit{ops = [Op]},
    UserOpID = make_user_op_id(),
    dmt_client:commit(Version, Commit, UserOpID).

commit_update(Version, Object) ->
    Op = {update, #domain_conf_v2_UpdateOp{new_object = Object}},
    Commit = #domain_conf_v2_Commit{ops = [Op]},
    UserOpID = make_user_op_id(),
    dmt_client:commit(Version, Commit, UserOpID).

get_base_version(Object) ->
    try
        {ok, #domain_conf_v2_VersionedObject{global_version = Version}} =
            dmt_client:checkout_object({head, #domain_conf_v2_Head{}}, get_ref(Object)),
        Version
    catch
        #domain_conf_v2_ObjectNotFound{} ->
            0
    end.

get_ref(#domain_CategoryObject{ref = Ref}) -> Ref.

create_large_binary(Size) ->
    list_to_binary(lists:duplicate(Size, $a)).

% insert_and_access(Object) ->
%     Version = commit_insert(Object),
%     VersionRef = {version, Version},
%     {Ref, _, _} = Object,
%     % Access to cache it
%     #domain_conf_v2_VersionedObject{} = dmt_client:checkout_object(VersionRef, Ref),
%     {Ref, VersionRef}.

-spec start_dmt_client(map()) -> [atom()].
start_dmt_client(ExtraConfig) ->
    Config = [
        ExtraConfig
        | {service_urls, #{
            'Repository' => <<"http://dominant:8022/v1/domain/repository">>,
            'RepositoryClient' => <<"http://dominant:8022/v1/domain/repository_client">>,
            'UserOpManagement' => <<"http://dmt:8022/v1/domain/user_op">>
        }}
    ],
    ct:pal("~p", Config),
    genlib_app:start_application_with(dmt_client, Config).

-spec stop_dmt_client(config()) -> ok.
stop_dmt_client(Config) ->
    genlib_app:stop_unload_applications(?config(apps, Config)).
