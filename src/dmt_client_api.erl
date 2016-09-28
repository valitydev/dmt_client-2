-module(dmt_client_api).

-export([commit/2]).
-export([checkout/1]).
-export([pull/1]).
-export([checkout_object/2]).


-spec commit(dmt:version(), dmt:commit()) -> dmt:version().
commit(Version, Commit) ->
    call(repository, 'Commit', [Version, Commit]).

-spec checkout(dmt:ref()) -> dmt:snapshot().
checkout(Reference) ->
    call(repository, 'Checkout', [Reference]).

-spec pull(dmt:version()) -> dmt:history().
pull(Version) ->
    call(repository, 'Pull', [Version]).

-spec checkout_object(dmt:ref(), dmt:object_ref()) -> dmt:domain_object().
checkout_object(Reference, ObjectReference) ->
    call(repository_client, 'checkoutObject', [Reference, ObjectReference]).


call(ServiceName, Function, Args) ->
    Host = application:get_env(dmt, client_host, "dominant"),
    Port = integer_to_list(application:get_env(dmt, client_port, 8022)),
    {Path, Service} = get_handler_spec(ServiceName),
    Call = {Service, Function, Args},
    Server = #{url => Host ++ ":" ++ Port ++ Path},
    Context = woody_client:new_context(woody_client:make_id(<<"dmt_client">>), dmt_client_woody_event_handler),
    case woody_client:call_safe(Context, Call, Server) of
        {{exception, Exception}, _Context} ->
            throw(Exception);
        {{error, Error}, _Context} ->
            error(Error);
        {{ok, Response}, _Context} -> %% woody compatibility hack
            Response;
        {Response, _Context} ->
            Response
    end.

get_handler_spec(repository) ->
    {"/v1/domain/repository",
        {dmt_client_domain_config_thrift, 'Repository'}};
get_handler_spec(repository_client) ->
    {"/v1/domain/repository_client",
        {dmt_client_domain_config_thrift, 'RepositoryClient'}}.
