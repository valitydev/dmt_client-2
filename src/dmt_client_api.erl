-module(dmt_client_api).

-behaviour(dmt_client_backend).

-export([commit/4]).
-export([checkout_object/3]).

-spec commit(
    dmt_client:vsn(),
    dmt_client:commit(),
    dmt_client:user_op_id(),
    dmt_client:opts()
) ->
    dmt_client:vsn() | no_return().
commit(Version, Commit, UserOpID, Opts) ->
    call('Repository', 'Commit', {Version, Commit, UserOpID}, Opts).

-spec checkout_object(dmt_client:object_ref(), dmt_client:vsn(), dmt_client:opts()) ->
    {ok, dmsl_domain_conf_v2_thrift:'VersionedObject'()} | no_return().
checkout_object(ObjectReference, VersionRef, Opts) ->
    call('RepositoryClient', 'CheckoutObject', {VersionRef, ObjectReference}, Opts).

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

    CallOpts =
        #{
            url => Url,
            event_handler => get_event_handlers(),
            transport_opts => TransportOpts
        },

    Context = maps:get(woody_context, Opts, woody_context:new()),

    io:format("Call, CallOpts, Context: ~p~n~p~n~p~n", [Call, CallOpts, Context]),

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
    dmsl_domain_conf_v2_thrift;
get_service_module('RepositoryClient') ->
    dmsl_domain_conf_v2_thrift.

get_event_handlers() ->
    genlib_app:env(dmt_client, woody_event_handlers, []).
