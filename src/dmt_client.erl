%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dmt_client).

-behaviour(supervisor).
-behaviour(application).

%% API
-export([checkout_object/1]).
-export([checkout_object/2]).
-export([checkout_object/3]).
-export([commit/3]).
-export([commit/4]).

% UserOpManagement API

-export([user_op_create/2]).
-export([user_op_get/1]).
-export([user_op_delete/1]).

%% Health check API
-export([health_check/0]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

-export_type([vsn/0]).
-export_type([vsn_created_at/0]).
-export_type([version/0]).
-export_type([base_version/0]).
-export_type([commit/0]).
-export_type([object_ref/0]).
-export_type([domain_object/0]).
-export_type([opts/0]).

-export_type([user_op_id/0]).
-export_type([user_op/0]).
-export_type([user_op_params/0]).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-type vsn() :: {version, non_neg_integer()} | {head, dmsl_domain_conf_v2_thrift:'Head'()}.
-type vsn_created_at() :: dmsl_base_thrift:'Timestamp'().
-type version() :: base_version() | latest.
-type base_version() :: non_neg_integer().
-type commit() :: dmsl_domain_conf_v2_thrift:'Commit'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type domain_object() :: dmsl_domain_thrift:'ReflessDomainObject'().
-type opts() :: #{
    transport_opts => woody_client_thrift_http_transport:transport_options(),
    woody_context => woody_context:ctx()
}.

-type user_op_id() :: dmsl_domain_conf_v2_thrift:'UserOpID'().
-type user_op() :: dmsl_domain_conf_v2_thrift:'UserOp'().
-type user_op_params() :: dmsl_domain_conf_v2_thrift:'UserOpParams'().

%%% API

-spec checkout_object(object_ref()) -> domain_object() | no_return().
checkout_object(ObjectReference) ->
    checkout_object(ObjectReference, latest).

-spec checkout_object(object_ref(), version()) -> domain_object() | no_return().
checkout_object(ObjectReference, Reference) ->
    checkout_object(ObjectReference, Reference, #{}).

-spec checkout_object(object_ref(), version(), opts()) -> domain_object() | no_return().
checkout_object(ObjectReference, Reference, Opts) ->
    unwrap(do_checkout_object(ObjectReference, Reference, Opts)).

do_checkout_object(ObjectReference, Reference, Opts) ->
    Version = ref_to_version(Reference),
    dmt_client_cache:get_object(ObjectReference, Version, Opts).

-spec commit(vsn(), commit(), user_op_id()) -> vsn() | no_return().
commit(Version, Commit, UserOpID) ->
    commit(Version, Commit, UserOpID, #{}).

-spec commit(vsn(), commit(), user_op_id(), opts()) -> vsn() | no_return().
commit(Version, Commit, UserOpID, Opts) ->
    dmt_client_backend:commit(Version, Commit, UserOpID, Opts).

% UserOpManagement

-spec user_op_create(user_op_params(), opts()) -> user_op().
user_op_create(Params, Opts) ->
    dmt_client_user_op:create(Params, Opts).

-spec user_op_get(user_op_id()) -> user_op().
user_op_get(ID) ->
    dmt_client_user_op:get(ID).

-spec user_op_delete(user_op_id()) -> ok.
user_op_delete(ID) ->
    dmt_client_user_op:delete(ID).

%% Health check API

-spec health_check() -> erl_health:result().
health_check() ->
    try
        % TODO Come up with healthcheck
        {passing, #{}}
    catch
        _Class:_Error ->
            {critical, #{last_version => not_found}}
    end.

%%% Supervisor callbacks

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    Cache = #{
        id => dmt_client_cache,
        start => {dmt_client_cache, start_link, []},
        restart => permanent
    },
    {ok,
        {
            #{
                strategy => one_for_one,
                intensity => 10,
                period => 60
            },
            [Cache]
        }}.

%%% Application callbacks

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.
start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) -> ok.
stop(_State) ->
    ok.

%%% Internal functions

unwrap({ok, Acc}) -> Acc;
unwrap({error, {woody_error, _} = Error}) -> erlang:error(Error);
unwrap({error, version_not_found = Reason}) -> erlang:error(Reason);
unwrap({error, object_not_found}) -> erlang:throw(#domain_conf_v2_ObjectNotFound{}).

-spec ref_to_version(version()) -> vsn().
ref_to_version(Version) when is_integer(Version) ->
    {version, Version};
ref_to_version(latest) ->
    {head, #domain_conf_v2_Head{}}.
