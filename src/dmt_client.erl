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

%% Health check API
-export([health_check/0]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

-export_type([vsn/0]).
-export_type([version/0]).
-export_type([commit/0]).
-export_type([object_ref/0]).
-export_type([domain_object/0]).
-export_type([user_op_id/0]).
-export_type([opts/0]).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-type vsn() :: dmsl_domain_conf_v2_thrift:'Version'().
-type version() :: vsn() | latest.
-type commit() :: dmsl_domain_conf_thrift:'Commit'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type domain_object() :: dmsl_domain_thrift:'ReflessDomainObject'().
-type user_op_id() :: dmsl_domain_conf_v2_thrift:'UserOpID'().
-type opts() :: #{
    transport_opts => woody_client_thrift_http_transport:transport_options(),
    woody_context => woody_context:ctx()
}.

%%% API

-spec checkout_object(object_ref()) -> domain_object() | no_return().
checkout_object(ObjectReference) ->
    checkout_object(latest, ObjectReference).

-spec checkout_object(version(), object_ref()) -> domain_object() | no_return().
checkout_object(Reference, ObjectReference) ->
    checkout_object(Reference, ObjectReference, #{}).

-spec checkout_object(version(), object_ref(), opts()) -> domain_object() | no_return().
checkout_object(Reference, ObjectReference, Opts) ->
    unwrap(do_checkout_object(Reference, ObjectReference, Opts)).

do_checkout_object(Reference, ObjectReference, Opts) ->
    Version = ref_to_version(Reference),
    dmt_client_cache:get_object(Version, ObjectReference, Opts).

-spec commit(vsn(), commit(), user_op_id()) -> vsn() | no_return().
commit(Version, Commit, UserOpID) ->
    commit(Version, Commit, UserOpID, #{}).

-spec commit(vsn(), commit(), user_op_id(), opts()) -> vsn() | no_return().
commit(Version, Commit, UserOpID, Opts) ->
    dmt_client_backend:commit(Version, Commit, UserOpID, Opts).

%% Health check API

-spec health_check() -> erl_health:result().
health_check() ->
    try
        _ = dmt_client_cache:get_last_version(),
        {passing, #{}}
    catch
        _Class:_Error ->
            {critical, #{last_version => not_found}}
    end.

%%% Supervisor callbacks

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    Cache = #{id => dmt_client_cache, start => {dmt_client_cache, start_link, []}, restart => permanent},
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, [Cache]}}.

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
