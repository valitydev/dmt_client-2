-module(dmt_client_user_op_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% Common test callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
%% Test cases
-export([
    create_user_op/1,
    get_user_op/1,
    delete_user_op/1,
    get_nonexistent_user_op/1,
    delete_nonexistent_user_op/1,
    create_duplicate_email/1
]).

-type config() :: ct_suite:ct_config().

%% CT callbacks

-spec all() -> [{group, atom()}].
all() ->
    [{group, basic_operations}, {group, error_cases}].

-spec groups() -> [{atom(), list(), [atom()]}].
groups() ->
    [
        {basic_operations, [], [create_user_op, get_user_op, delete_user_op]},
        {error_cases, [parallel], [
            get_nonexistent_user_op, delete_nonexistent_user_op, create_duplicate_email
        ]}
    ].

%% Group callbacks

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
    Apps0 = genlib_app:start_application(woody),
    Apps1 = start_dmt_client(),
    % ok = application:get_all_env(dmt_client),
    [{apps, Apps0 ++ Apps1} | Config].

-spec end_per_suite(config()) -> ok.
end_per_suite(Config) ->
    stop_dmt_client(Config),
    ok.

%% Test cases

-spec create_user_op(config()) -> _.
create_user_op(_Config) ->
    % ok = dmt_client:checkout_object({category, #domain_CategoryRef{id = 1}}),
    Email = generate_email(),
    Name = generate_name(),
    Params = #domain_conf_v2_UserOpParams{email = Email, name = Name},
    UserOp = dmt_client_user_op:create(Params, #{}),
    ?assertMatch(
        #domain_conf_v2_UserOp{
            id = _,
            email = Email,
            name = Name
        },
        UserOp
    ).

-spec get_user_op(config()) -> _.
get_user_op(_Config) ->
    % Create user op first
    Params = #domain_conf_v2_UserOpParams{email = generate_email(), name = generate_name()},
    #domain_conf_v2_UserOp{id = ID} = dmt_client_user_op:create(Params, #{}),

    % Get and verify
    UserOp = dmt_client_user_op:get(ID),
    ?assertEqual(
        Params#domain_conf_v2_UserOpParams.email,
        UserOp#domain_conf_v2_UserOp.email
    ),
    ?assertEqual(Params#domain_conf_v2_UserOpParams.name, UserOp#domain_conf_v2_UserOp.name).

-spec delete_user_op(config()) -> _.
delete_user_op(_Config) ->
    % Create user op
    Params = #domain_conf_v2_UserOpParams{email = generate_email(), name = generate_name()},
    #domain_conf_v2_UserOp{id = ID} = dmt_client_user_op:create(Params, #{}),

    % Delete
    ?assertEqual(ok, dmt_client_user_op:delete(ID)),

    % Verify it's gone
    ?assertThrow(#domain_conf_v2_UserOpNotFound{}, dmt_client_user_op:get(ID)).

-spec get_nonexistent_user_op(config()) -> _.
get_nonexistent_user_op(_Config) ->
    ?assertThrow(
        #domain_conf_v2_UserOpNotFound{},
        dmt_client_user_op:get(<<"nonexistent_id">>)
    ).

-spec delete_nonexistent_user_op(config()) -> _.
delete_nonexistent_user_op(_Config) ->
    ?assertThrow(
        #domain_conf_v2_UserOpNotFound{},
        dmt_client_user_op:delete(<<"nonexistent_id">>)
    ).

-spec create_duplicate_email(config()) -> _.
create_duplicate_email(_Config) ->
    Email = generate_email(),
    Params1 = #domain_conf_v2_UserOpParams{email = Email, name = generate_name()},
    _UserOp1 = dmt_client_user_op:create(Params1, #{}),

    % Try to create another user op with same email
    Params2 = #domain_conf_v2_UserOpParams{email = Email, name = generate_name()},
    ?assertThrow(
        #domain_conf_v2_UserAlreadyExists{},
        dmt_client_user_op:create(Params2, #{})
    ).

%% Internal functions

-spec start_dmt_client() -> [atom()].
start_dmt_client() ->
    genlib_app:start_application_with(dmt_client, [
        {service_urls, #{
            'Repository' => <<"http://dmt:8022/v1/domain/repository">>,
            'RepositoryClient' => <<"http://dmt:8022/v1/domain/repository_client">>,
            'UserOpManagement' => <<"http://dmt:8022/v1/domain/user_op">>
        }}
    ]).

-spec stop_dmt_client(config()) -> ok.
stop_dmt_client(Config) ->
    genlib_app:stop_unload_applications(?config(apps, Config)).

generate_email() ->
    ID = genlib:unique(),
    erlang:iolist_to_binary([ID, <<"@example.com">>]).

generate_name() ->
    ID = genlib:unique(),
    erlang:iolist_to_binary([<<"User ">>, ID]).
