-module(dmt_client_tests_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% Common test callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases
-export([
    checkout_nonexistent_object/1,
    checkout_object_by_version/1,
    checkout_latest_object/1,
    commit_insert_object/1,
    commit_update_object/1,
    % commit_remove_object/1,
    commit_multiple_operations/1,
    version_sequence_operations/1,
    commit_conflict_handling/1
]).

-type config() :: ct_suite:ct_config().

%% CT callbacks

-spec all() -> [{group, atom()}].
all() ->
    [
        {group, sequential_operations},
        {group, parallel_operations}
    ].

-spec groups() -> [{atom(), list(), [atom()]}].
groups() ->
    [
        {sequential_operations, [], [
            checkout_latest_object,
            version_sequence_operations
        ]},
        {parallel_operations, [parallel], [
            checkout_nonexistent_object,
            checkout_object_by_version,
            commit_insert_object,
            commit_update_object,
            % commit_remove_object,
            commit_multiple_operations,
            commit_conflict_handling
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
    Apps = genlib_app:start_application_with(dmt_client, [
        {max_cache_size, #{
            elements => 20,
            % 50MB
            memory => 52428800
        }},
        {service_urls, #{
            'Repository' => <<"http://dmt:8022/v1/domain/repository">>,
            'RepositoryClient' => <<"http://dmt:8022/v1/domain/repository_client">>,
            'UserOpManagement' => <<"http://dmt:8022/v1/domain/user_op">>
        }}
    ]),
    [{apps, Apps} | Config].

-spec end_per_suite(config()) -> any().
end_per_suite(Config) ->
    genlib_app:stop_unload_applications(?config(apps, Config)).

%% Tests

-spec checkout_nonexistent_object(config()) -> _.
checkout_nonexistent_object(_Config) ->
    Ref = make_domain_ref(),
    ?assertThrow(
        #domain_conf_v2_ObjectNotFound{},
        dmt_client:checkout_object(Ref)
    ).

-spec checkout_object_by_version(config()) -> _.
checkout_object_by_version(_Config) ->
    {Ref, RLObject, Object} = make_test_object(make_domain_ref()),
    #domain_conf_v2_CommitResponse{version = Version} =
        commit_insert(RLObject, Ref),

    % Check we can get object by specific version
    #domain_conf_v2_VersionedObject{
        object = Result
    } = dmt_client:checkout_object(Ref, Version),
    ?assertEqual(Object, Result).

-spec checkout_latest_object(config()) -> _.
checkout_latest_object(_Config) ->
    {Ref, RLObject, Object} = make_test_object(make_domain_ref()),
    _Version = commit_insert(RLObject, Ref),

    % Check we can get latest version
    #domain_conf_v2_VersionedObject{
        object = Result
    } = dmt_client:checkout_object(Ref, latest),
    ?assertEqual(Object, Result).

-spec commit_insert_object(config) -> _.
commit_insert_object(_Config) ->
    {Ref, ROObject, Object} = make_test_object(make_domain_ref()),
    #domain_conf_v2_CommitResponse{version = Version} =
        commit_insert(ROObject, Ref),
    ?assert(is_integer(Version)),

    #domain_conf_v2_VersionedObject{
        object = Result
    } = dmt_client:checkout_object(Ref),
    ?assertEqual(Object, Result).

-spec commit_update_object(config()) -> _.
commit_update_object(_Config) ->
    {Ref, ROObject, _} = make_test_object(make_domain_ref()),
    #domain_conf_v2_CommitResponse{version = Version1} =
        commit_insert(ROObject, Ref),

    {_, _, Object2} = make_test_object(Ref, <<"new_name">>),
    #domain_conf_v2_CommitResponse{version = Version2} =
        commit_update(Version1, Ref, Object2),
    ?assert(Version2 > Version1),

    #domain_conf_v2_VersionedObject{
        object = Result
    } = dmt_client:checkout_object(Ref),
    ?assertEqual(Object2, Result).

% TODO Remove doesn't work yet

% -spec commit_remove_object(config()) -> _.
% commit_remove_object(_Config) ->
%     {Ref, ROObject, _} = make_test_object(make_domain_ref()),
%     #domain_conf_v2_CommitResponse{version = Version1} =
%         commit_insert(ROObject, Ref),

%     #domain_conf_v2_CommitResponse{version = Version2} =
%         commit_remove(Version1, Ref),
%     ?assert(Version2 > Version1),

%     ?assertThrow(
%         #domain_conf_v2_ObjectNotFound{},
%         dmt_client:checkout_object(Ref, Version2)
%     ).

-spec commit_multiple_operations(config()) -> _.
commit_multiple_operations(_Config) ->
    {Ref1, _, Object1} = Obj1 = make_test_object(make_domain_ref()),
    {Ref2, _, Object2} = Obj2 = make_test_object(make_domain_ref()),

    #domain_conf_v2_CommitResponse{} =
        commit_batch_insert([Obj1, Obj2]),

    #domain_conf_v2_VersionedObject{
        object = Result1
    } = dmt_client:checkout_object(Ref1),
    #domain_conf_v2_VersionedObject{
        object = Result2
    } = dmt_client:checkout_object(Ref2),
    ?assertEqual(Object1, Result1),
    ?assertEqual(Object2, Result2).

-spec commit_conflict_handling(config()) -> _.
commit_conflict_handling(_Config) ->
    {Ref, RLObject, _} = make_test_object(make_domain_ref()),
    #domain_conf_v2_CommitResponse{version = Version1} =
        commit_insert(RLObject, Ref),

    % Try to insert object with same reference
    ?assertThrow(
        #domain_conf_v2_OperationConflict{
            conflict =
                {object_already_exists, #domain_conf_v2_ObjectAlreadyExistsConflict{
                    object_ref = Ref
                }}
        },
        commit_insert(Version1, RLObject, Ref)
    ).

-spec version_sequence_operations(config()) -> _.
version_sequence_operations(_Config) ->
    {Ref, RLObject, Object1} = make_test_object(make_domain_ref()),

    % First insert

    #domain_conf_v2_CommitResponse{version = Version1} =
        commit_insert(RLObject, Ref),
    #domain_conf_v2_VersionedObject{object = Result1} =
        dmt_client:checkout_object(Ref, latest),
    {category, #domain_CategoryObject{data = Data1}} = Result1,
    ?assertEqual(Object1, Result1),

    % Update
    {category, IRef} = Ref,
    Object2 =
        {category, #domain_CategoryObject{
            ref = IRef,
            data = Data1#domain_Category{
                description = <<"new_desc">>
            }
        }},
    Version2 = commit_update(Version1, Ref, Object2),
    ?assert(Version2 > Version1),

    #domain_conf_v2_VersionedObject{object = Result2} =
        dmt_client:checkout_object(Ref, latest),
    ?assertEqual(Object2, Result2),

    % Verify we can still get old version

    #domain_conf_v2_VersionedObject{object = Historical} =
        dmt_client:checkout_object(Ref, Version1),
    ?assertEqual(Object1, Historical).

%% Internal functions

make_domain_ref() ->
    {category, #domain_CategoryRef{id = rand:uniform(10000)}}.

make_test_object({category, #domain_CategoryRef{id = ID}} = Ref) ->
    Name = erlang:integer_to_binary(ID),
    make_test_object(Ref, Name).

make_test_object({category, CategoryRef} = FullRef, Name) ->
    Category = #domain_Category{
        name = Name,
        description = <<"Test category">>
    },
    ReflessObject = {category, Category},
    Object = {category, #domain_CategoryObject{ref = CategoryRef, data = Category}},
    {FullRef, ReflessObject, Object}.

make_user_op_id() ->
    Params = #domain_conf_v2_UserOpParams{email = genlib:unique(), name = genlib:unique()},
    #domain_conf_v2_UserOp{id = ID} =
        dmt_client_user_op:create(Params, #{}),
    ID.

commit_insert(Object, Ref) ->
    commit_insert(1, Object, Ref).

commit_insert(Version, Object, Ref) ->
    % Get version from any existing object or start with 0
    Op = {insert, #domain_conf_v2_InsertOp{object = Object, force_ref = Ref}},
    Commit = #domain_conf_v2_Commit{ops = [Op]},
    UserOpID = make_user_op_id(),
    dmt_client:commit(Version, Commit, UserOpID).

commit_update(Version, Ref, Object) ->
    Op = {update, #domain_conf_v2_UpdateOp{targeted_ref = Ref, new_object = Object}},
    Commit = #domain_conf_v2_Commit{ops = [Op]},
    UserOpID = make_user_op_id(),
    dmt_client:commit(Version, Commit, UserOpID).

% commit_remove(Version, Ref) ->
%     Op = {remove, #domain_conf_v2_RemoveOp{ref = Ref}},
%     Commit = #domain_conf_v2_Commit{ops = [Op]},
%     UserOpID = make_user_op_id(),
%     dmt_client:commit(Version, Commit, UserOpID).

commit_batch_insert(Objects) ->
    Version = 1,
    Ops = [
        {insert, #domain_conf_v2_InsertOp{
            object = Obj,
            force_ref = Ref
        }}
     || {Ref, Obj, _} <- Objects
    ],
    Commit = #domain_conf_v2_Commit{ops = Ops},
    UserOpID = make_user_op_id(),
    dmt_client:commit(Version, Commit, UserOpID).
