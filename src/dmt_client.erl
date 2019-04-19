%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dmt_client).
-behaviour(supervisor).
-behaviour(application).

%% API
-export([checkout/1]).
-export([checkout/2]).
-export([checkout_object/2]).
-export([checkout_object/3]).
-export([commit/2]).
-export([commit/3]).
-export([pull_range/2]).
-export([pull_range/3]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

-export_type([ref/0]).
-export_type([version/0]).
-export_type([limit/0]).
-export_type([snapshot/0]).
-export_type([commit/0]).
-export_type([object_ref/0]).
-export_type([history/0]).
-export_type([transport_opts/0]).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-type ref() :: dmsl_domain_config_thrift:'Reference'().
-type version() :: dmsl_domain_config_thrift:'Version'().
-type limit() :: dmsl_domain_config_thrift:'Limit'().
-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().
-type commit() :: dmsl_domain_config_thrift:'Commit'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type history() :: dmsl_domain_config_thrift:'History'().
-type transport_opts() :: woody_client_thrift_http_transport:transport_options() | undefined.

%% API

-spec checkout(ref()) ->
    snapshot() | no_return().

checkout(Reference) ->
    checkout(Reference, undefined).

-spec checkout(ref(), transport_opts()) ->
    snapshot() | no_return().

checkout(Reference, Opts) ->
    CacheResult = case Reference of
        {head, #'Head'{}} ->
            dmt_client_cache:get_latest();
        {version, Version} ->
            dmt_client_cache:get(Version)
    end,
    case CacheResult of
        {ok, Snapshot} ->
            Snapshot;
        {error, version_not_found} ->
            dmt_client_cache:put(dmt_client_api:checkout(Reference, Opts))
    end.

-spec checkout_object(ref(), object_ref()) ->
    dmsl_domain_config_thrift:'VersionedObject'() | no_return().

checkout_object(Reference, ObjectReference) ->
    checkout_object(Reference, ObjectReference, undefined).

-spec checkout_object(ref(), object_ref(), transport_opts()) ->
    dmsl_domain_config_thrift:'VersionedObject'() | no_return().

checkout_object(Reference, ObjectReference, Opts) ->
    #'Snapshot'{version = Version, domain = Domain} = checkout(Reference, Opts),
    case dmt_domain:get_object(ObjectReference, Domain) of
        {ok, Object} ->
            #'VersionedObject'{version = Version, object = Object};
        error ->
            throw(#'ObjectNotFound'{})
    end.

-spec commit(version(), commit()) ->
    version() | no_return().

commit(Version, Commit) ->
    commit(Version, Commit, undefined).

-spec commit(version(), commit(), transport_opts()) ->
    version() | no_return().

commit(Version, Commit, Opts) ->
    dmt_client_api:commit(Version, Commit, Opts).

-spec pull_range(version(), limit()) ->
    history() | no_return().

pull_range(Version, Limit) ->
    pull_range(Version, Limit, undefined).

-spec pull_range(version(), limit(), transport_opts()) ->
    history() | no_return().

pull_range(Version, Limit, Opts) ->
    dmt_client_api:pull_range(Version, Limit, Opts).

%% Supervisor callbacks

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init([]) ->
    Cache = #{id => dmt_client_cache, start => {dmt_client_cache, start_link, []}, restart => permanent},
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, [Cache]}}.

%% Application callbacks

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.

start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) -> ok.

stop(_State) ->
    ok.
