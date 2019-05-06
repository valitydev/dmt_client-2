-module(dmt_client_api).

-behaviour(dmt_client_backend).

-export([commit/3]).
-export([checkout/2]).
-export([pull_range/3]).
-export([checkout_object/3]).

-spec commit(dmt_client:version(), dmt_client:commit(), dmt_client:transport_opts()) ->
    dmt_client:version() | no_return().

commit(Version, Commit, Opts) ->
    call('Repository', 'Commit', [Version, Commit], Opts).

-spec checkout(dmt_client:ref(), dmt_client:transport_opts()) ->
    dmt_client:snapshot() | no_return().

checkout(Reference, Opts) ->
    call('Repository', 'Checkout', [Reference], Opts).

-spec pull_range(dmt_client:version(), dmt_client:limit(), dmt_client:transport_opts()) ->
    dmt_client:history() | no_return().

pull_range(After, Limit, Opts) ->
    call('Repository', 'PullRange', [After, Limit], Opts).

-spec checkout_object(dmt_client:ref(), dmt_client:object_ref(), dmt_client:transport_opts()) ->
    dmsl_domain_thrift:'DomainObject'() | no_return().

checkout_object(Reference, ObjectReference, Opts) ->
    call('RepositoryClient', 'checkoutObject', [Reference, ObjectReference], Opts).


call(ServiceName, Function, Args, TransportOpts) ->
    Url = get_service_url(ServiceName),
    Service = get_service_modname(ServiceName),
    Call = {Service, Function, Args},
    Opts = #{
        url => Url,
        event_handler => scoper_woody_event_handler,
        transport_opts => ensure_transport_opts(TransportOpts)
    },
    Context = woody_context:new(),
    case woody_client:call(Call, Opts, Context) of
        {ok, Response} ->
            Response;
        {exception, Exception} ->
            throw(Exception)
    end.

get_service_url(ServiceName) ->
    maps:get(ServiceName, genlib_app:env(dmt_client, service_urls)).

get_service_modname(ServiceName) ->
    {get_service_module(ServiceName), ServiceName}.

get_service_module('Repository') ->
    dmsl_domain_config_thrift;
get_service_module('RepositoryClient') ->
    dmsl_domain_config_thrift.

-spec ensure_transport_opts(dmt_client:transport_opts()) ->
    woody_client_thrift_http_transport:options().

ensure_transport_opts(Opts) when is_list(Opts) ->
    Opts;
ensure_transport_opts(undefined) ->
    Default = [{recv_timeout, 60000}, {connect_timeout, 1000}],
    genlib_app:env(dmt_client, transport_opts, Default).
