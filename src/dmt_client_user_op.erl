-module(dmt_client_user_op).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

%% API
-export([
    create/2,
    get/1,
    delete/1
]).

-export_type([user_op/0]).
-export_type([user_op_params/0]).
-export_type([user_op_id/0]).
-export_type([opts/0]).

-type user_op() :: dmt_client:user_op().
-type user_op_params() :: dmt_client:user_op_params().
-type user_op_id() :: dmt_client:user_op_id().
-type opts() :: dmt_client:opts().

-spec create(user_op_params(), opts()) -> user_op() | no_return().
create(Params, Opts) ->
    call('UserOpManagement', 'Create', {Params}, Opts).

-spec get(user_op_id()) -> user_op() | no_return().
get(ID) ->
    get(ID, #{}).

-spec get(user_op_id(), opts()) -> user_op() | no_return().
get(ID, Opts) ->
    call('UserOpManagement', 'Get', {ID}, Opts).

-spec delete(user_op_id()) -> ok | no_return().
delete(ID) ->
    delete(ID, #{}).

-spec delete(user_op_id(), opts()) -> ok | no_return().
delete(ID, Opts) ->
    call('UserOpManagement', 'Delete', {ID}, Opts).

%% Internal functions

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

    case woody_client:call(Call, CallOpts, Context) of
        {ok, Response} ->
            Response;
        {exception, Exception} ->
            throw(Exception)
    end.

get_service_url(ServiceName) ->
    case genlib_app:env(dmt_client, service_urls) of
        undefined ->
            error({config_missing, service_urls});
        Config when is_map(Config) ->
            case maps:get(ServiceName, Config, undefined) of
                undefined ->
                    error({service_url_missing, ServiceName});
                Url ->
                    Url
            end
    end.

get_service_modname(ServiceName) ->
    {get_service_module(ServiceName), ServiceName}.

get_service_module('UserOpManagement') ->
    dmsl_domain_conf_v2_thrift.

get_event_handlers() ->
    genlib_app:env(dmt_client, woody_event_handlers, [woody_event_handler_default]).
