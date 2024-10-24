-module(dmt_client_cache_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-behaviour(dmt_client_backend).

-export([commit/3]).
-export([checkout/2]).
-export([checkout_object/3]).
-export([pull_range/3]).

-export([get_snapshot_success/1]).
-export([snapshot_not_found/1]).
-export([woody_error/1]).
-export([object_not_found/1]).

-include_lib("damsel/include/dmsl_domain_conf_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

-define(NOTFOUND_VERSION, 1).
-define(UNAVAILABLE_VERSION, 2).
-define(EXISTING_VERSION, 42).

-type config() :: [tuple()].
-type testcase_or_group() :: atom() | {group, atom()}.

-define(TESTS_COUNT, 100).

%%% Common test callbacks

-spec all() -> [testcase_or_group()].
all() ->
    [
        {group, main}
    ].

-spec groups() -> [{atom(), list(), [testcase_or_group()]}].
groups() ->
    [
        {main, [parallel, shuffle], [
            {group, get_snapshot_success},
            {group, snapshot_not_found},
            {group, woody_error},
            {group, object_not_found},
            {group, mixed}
        ]},
        {get_snapshot_success, [parallel], lists:duplicate(?TESTS_COUNT, get_snapshot_success)},
        {snapshot_not_found, [parallel], lists:duplicate(?TESTS_COUNT, snapshot_not_found)},
        {woody_error, [parallel], lists:duplicate(?TESTS_COUNT, woody_error)},
        {object_not_found, [parallel], lists:duplicate(?TESTS_COUNT, object_not_found)},
        {mixed, [parallel, shuffle],
            lists:duplicate(?TESTS_COUNT, get_snapshot_success) ++
                lists:duplicate(?TESTS_COUNT, snapshot_not_found) ++
                lists:duplicate(?TESTS_COUNT, woody_error) ++
                lists:duplicate(?TESTS_COUNT, object_not_found)}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = genlib_app:start_application_with(dmt_client, [
        {api_module, ?MODULE},
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

-spec end_per_suite(config()) -> any().
end_per_suite(C) ->
    genlib_app:stop_unload_applications(?config(apps, C)).

%%% Dummy API

-spec commit(dmt_client:vsn(), dmt_client:commit(), dmt_client:opts()) -> dmt_client:vsn() | no_return().
commit(Version, _Commit, _Opts) ->
    Version.

-spec checkout(dmt_client:ref(), dmt_client:opts()) -> dmt_client:snapshot() | no_return().
checkout({version, ?NOTFOUND_VERSION}, _Opts) ->
    erlang:throw(#domain_conf_VersionNotFound{});
checkout({version, ?UNAVAILABLE_VERSION}, _Opts) ->
    woody_error:raise(system, {external, resource_unavailable, <<"test">>});
checkout({version, ?EXISTING_VERSION}, _Opts) ->
    timer:sleep(5000),
    #domain_conf_Snapshot{version = ?EXISTING_VERSION, domain = dmt_domain:new()};
checkout({head, #domain_conf_Head{}}, _Opts) ->
    timer:sleep(5000),
    #domain_conf_Snapshot{version = ?EXISTING_VERSION, domain = dmt_domain:new()}.

-spec checkout_object(dmt_client:ref(), dmt_client:object_ref(), dmt_client:opts()) ->
    no_return().
checkout_object(_Reference, _ObjectReference, _Opts) ->
    erlang:throw(#domain_conf_ObjectNotFound{}).

-spec pull_range(dmt_client:vsn(), dmt_client:limit(), dmt_client:opts()) -> dmt_client:history() | no_return().
pull_range(_Version, _Limit, _Opts) ->
    timer:sleep(5000),
    #{}.

%%% Tests

-spec object_not_found(config()) -> any().
object_not_found(_C) ->
    Ref = {category, #domain_CategoryRef{id = 1}},
    {error, version_not_found} = dmt_client_cache:get_object(?NOTFOUND_VERSION, Ref, #{}),
    {error, object_not_found} = dmt_client_cache:get_object(?EXISTING_VERSION, Ref, #{}).
