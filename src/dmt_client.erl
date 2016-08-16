%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dmt_client).
-behaviour(supervisor).
-behaviour(application).

%% API
-export([checkout/1]).
-export([checkout_object/2]).
-export([commit/2]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

-include_lib("dmt/include/dmt_domain_config_thrift.hrl").

%% API

-spec checkout(dmt:ref()) -> dmt:snapshot().
checkout(Reference) ->
    try 
        dmt_cache:checkout(Reference)
    catch
        {version_not_found, Version} ->
            dmt_cache:cache(Version, dmt_client_api:pull(0))
    end.

-spec checkout_object(dmt:ref(), dmt:object_ref()) ->
    dmt_domain_config_thrift:'VersionedObject'().
checkout_object(Reference, ObjectReference) ->
    #'Snapshot'{version = Version, domain = Domain} = checkout(Reference),
    Object = dmt_domain:get_object(ObjectReference, Domain),
    #'VersionedObject'{version = Version, object = Object}.

-spec commit(dmt:version(), dmt:commit()) -> dmt:version().
commit(Version, Commit) ->
    dmt_client_api:commit(Version, Commit).

%% Supervisor callbacks

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init([]) ->
    Poller = #{id => dmt_client_poller, start => {dmt_client_poller, start_link, []}, restart => permanent},
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, [Poller]}}.

%% Application callbacks

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.

start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) -> ok.

stop(_State) ->
    ok.
