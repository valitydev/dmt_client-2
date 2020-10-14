-module(dmt_client_backend).

-export([commit/3]).
-export([checkout/2]).
-export([checkout_object/3]).
-export([pull_range/3]).

%%% Behaviour callbacks

-callback commit(dmt_client:version(), dmt_client:commit(), dmt_client:transport_opts()) ->
    dmt_client:version() | no_return().

-callback checkout(dmt_client:ref(), dmt_client:transport_opts()) -> dmt_client:snapshot() | no_return().

-callback checkout_object(dmt_client:ref(), dmt_client:object_ref(), dmt_client:transport_opts()) ->
    dmsl_domain_thrift:'DomainObject'() | no_return().

-callback pull_range(dmt_client:version(), dmt_client:limit(), dmt_client:transport_opts()) ->
    dmt_client:history() | no_return().

%%% API

-spec commit(dmt_client:version(), dmt_client:commit(), dmt_client:transport_opts()) ->
    dmt_client:version() | no_return().
commit(Version, Commit, Opts) ->
    call(commit, [Version, Commit, Opts]).

-spec checkout(dmt_client:ref(), dmt_client:transport_opts()) -> dmt_client:snapshot() | no_return().
checkout(Reference, Opts) ->
    call(checkout, [Reference, Opts]).

-spec checkout_object(dmt_client:ref(), dmt_client:object_ref(), dmt_client:transport_opts()) ->
    dmsl_domain_thrift:'DomainObject'() | no_return().
checkout_object(Reference, ObjectReference, Opts) ->
    call(checkout_object, [Reference, ObjectReference, Opts]).

-spec pull_range(dmt_client:version(), dmt_client:limit(), dmt_client:transport_opts()) ->
    dmt_client:history() | no_return().
pull_range(After, Limit, Opts) ->
    call(pull_range, [After, Limit, Opts]).

%%% Internal functions

-spec get_api_module() -> module().
get_api_module() ->
    genlib_app:env(dmt_client, api_module, dmt_client_api).

-spec call(atom(), list()) -> term() | no_return().
call(Fun, Args) ->
    Module = get_api_module(),
    erlang:apply(Module, Fun, Args).
