-module(dmt_client_tests_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([poll/1]).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

%%
%% tests descriptions
%%
-spec all() -> [term()].
all() ->
    [
        {group, basic_lifecycle}
    ].

-spec groups() -> [term()].
groups() ->
    [
        {basic_lifecycle, [sequence], [
            poll
        ]}
    ].

%%
%% starting/stopping
-spec init_per_suite(term()) -> term().
init_per_suite(C) ->
    Apps = genlib_app:start_application_with(dmt_client, [
        % milliseconds
        {cache_update_interval, 5000},
        {max_cache_size, #{
            elements => 1,
            % 2Kb
            memory => 2048
        }},
        {service_urls, #{
            'Repository' => <<"http://dominant:8022/v1/domain/repository">>,
            'RepositoryClient' => <<"http://dominant:8022/v1/domain/repository_client">>
        }}
    ]),
    [{apps, Apps} | C].

-spec end_per_suite(term()) -> term().
end_per_suite(C) ->
    genlib_app:stop_unload_applications(proplists:get_value(apps, C)).

%%
%% tests
-spec poll(term()) -> term().
poll(_C) ->
    Object = fixture_domain_object(1, <<"InsertFixture">>),
    Ref = fixture_object_ref(1),
    #'ObjectNotFound'{} = (catch dmt_client:checkout_object({head, #'Head'{}}, Ref)),
    #'Snapshot'{version = Version1} = dmt_client:checkout({head, #'Head'{}}),
    Version2 = dmt_client_api:commit(Version1, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}, undefined),
    true = Version1 < Version2,
    _ = dmt_client_cache:update(),
    #'Snapshot'{version = Version2} = dmt_client:checkout({head, #'Head'{}}),
    #'VersionedObject'{object = Object} = dmt_client:checkout_object({head, #'Head'{}}, Ref).

fixture_domain_object(Ref, Data) ->
    {category, #'domain_CategoryObject'{
        ref = #'domain_CategoryRef'{id = Ref},
        data = #'domain_Category'{name = Data, description = Data}
    }}.

fixture_object_ref(Ref) ->
    {category, #'domain_CategoryRef'{id = Ref}}.
