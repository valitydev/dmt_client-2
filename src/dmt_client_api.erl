-module(dmt_client_api).

-behaviour(dmt_client_backend).

-export([commit/3]).
-export([checkout/2]).
-export([pull_range/3]).
-export([checkout_object/3]).

-spec commit(dmt_client:vsn(), dmt_client:commit(), dmt_client:opts()) -> dmt_client:vsn() | no_return().
commit(Version, Commit, Opts) ->
    call('Repository', 'Commit', {Version, Commit}, Opts).

-spec checkout(dmt_client:ref(), dmt_client:opts()) -> dmt_client:snapshot() | no_return().
checkout(Reference, Opts) ->
    call('Repository', 'Checkout', {Reference}, Opts).

-spec pull_range(dmt_client:vsn(), dmt_client:limit(), dmt_client:opts()) -> dmt_client:history() | no_return().
pull_range(After, Limit, Opts) ->
    call('Repository', 'PullRange', {After, Limit}, Opts).

-spec checkout_object(dmt_client:ref(), dmt_client:object_ref(), dmt_client:opts()) ->
    dmsl_domain_thrift:'DomainObject'() | no_return().
checkout_object(Reference, ObjectReference, Opts) ->
    call('RepositoryClient', 'checkoutObject', {Reference, ObjectReference}, Opts).

call(ServiceName, Function, Args, Opts) ->
    Url = get_service_url(ServiceName),
    Service = get_service_modname(ServiceName),
    Call = {Service, Function, Args},
    TransportOpts =
        maps:merge(
            #{recv_timeout => 60000, connect_timeout => 1000},
            maps:merge(
                genlib_app:env(dmt_client, transport_opts, #{}),
                maps:get(transport_opts, Opts, #{})
            )
        ),

    CallOpts = #{
        url => Url,
        event_handler => get_event_handlers(),
        transport_opts => TransportOpts
    },

    Context =
        case maps:find(woody_context, Opts) of
            error -> woody_context:new();
            {ok, Ctx} -> Ctx
        end,

    case woody_client:call(Call, CallOpts, Context) of
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

get_event_handlers() ->
    genlib_app:env(dmt_client, woody_event_handlers, []).
