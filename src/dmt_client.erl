%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dmt_client).

-behaviour(supervisor).
-behaviour(application).

%% API
-export([get_snapshot/1]).
-export([get_snapshot/2]).
-export([get/2]).
-export([get/3]).
-export([get_versioned/2]).
-export([get_versioned/3]).
-export([get_by_type/2]).
-export([get_by_type/3]).
-export([filter/2]).
-export([filter/3]).
-export([fold/3]).
-export([fold/4]).
-export([commit/2]).
-export([commit/3]).
-export([get_last_version/0]).
-export([pull_range/2]).
-export([pull_range/3]).

%% Health check API
-export([health_check/0]).

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
-export_type([object_type/0]).
-export_type([object_filter/0]).
-export_type([object_folder/1]).
-export_type([domain_object/0]).
-export_type([domain/0]).
-export_type([history/0]).
-export_type([transport_opts/0]).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

-type ref() :: dmsl_domain_config_thrift:'Reference'().
-type version() :: dmsl_domain_config_thrift:'Version'().
-type limit() :: dmsl_domain_config_thrift:'Limit'().
-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().
-type commit() :: dmsl_domain_config_thrift:'Commit'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type object_type() :: atom().
-type object_filter() :: fun((object_type(), domain_object()) -> boolean()).
-type object_folder(Acc) :: fun((object_type(), domain_object(), Acc) -> Acc).
-type domain_object() :: dmsl_domain_thrift:'DomainObject'().
-type versioned_object() :: dmsl_domain_config_thrift:'VersionedObject'().
-type domain() :: dmsl_domain_thrift:'Domain'().
-type history() :: dmsl_domain_config_thrift:'History'().
-type transport_opts() :: woody_client_thrift_http_transport:transport_options() | undefined.

%%% API

-spec get_snapshot(ref()) -> snapshot() | no_return().
get_snapshot(Reference) ->
    get_snapshot(Reference, undefined).

-spec get_snapshot(ref(), transport_opts()) -> snapshot() | no_return().
get_snapshot(Reference, Opts) ->
    Version = ref_to_version(Reference),
    case dmt_client_cache:get_snapshot(Version, Opts) of
        {ok, Snapshot} ->
            Snapshot;
        {error, Error} ->
            erlang:error(Error)
    end.

-spec get(ref(), object_ref()) -> domain_object() | no_return().
get(Reference, ObjectReference) ->
    get(Reference, ObjectReference, undefined).

-spec get(ref(), object_ref(), transport_opts()) -> domain_object() | no_return().
get(Reference, ObjectReference, Opts) ->
    Version = ref_to_version(Reference),
    unwrap(dmt_client_cache:get_object(Version, ObjectReference, Opts)).

-spec get_versioned(ref(), object_ref()) -> versioned_object() | no_return().
get_versioned(Reference, ObjectReference) ->
    get_versioned(Reference, ObjectReference, undefined).

-spec get_versioned(ref(), object_ref(), transport_opts()) -> versioned_object() | no_return().
get_versioned(Reference, ObjectReference, Opts) ->
    Version = ref_to_version(Reference),
    #'VersionedObject'{version = Version, object = get(Reference, ObjectReference, Opts)}.

-spec get_by_type(ref(), object_type()) -> [domain_object()] | no_return().
get_by_type(Reference, ObjectType) ->
    get_by_type(Reference, ObjectType, undefined).

-spec get_by_type(ref(), object_type(), transport_opts()) -> [domain_object()] | no_return().
get_by_type(Reference, ObjectType, Opts) ->
    Version = ref_to_version(Reference),
    unwrap(dmt_client_cache:get_by_type(Version, ObjectType, Opts)).

-spec filter(ref(), object_filter()) -> [{object_type(), domain_object()}] | no_return().
filter(Reference, Filter) ->
    filter(Reference, Filter, undefined).

-spec filter(ref(), object_filter(), transport_opts()) -> [{object_type(), domain_object()}] | no_return().
filter(Reference, Filter, Opts) ->
    Folder = fun(Type, Object, Acc) ->
        case Filter(Type, Object) of
            true -> [{Type, Object} | Acc];
            false -> Acc
        end
    end,
    fold(Reference, Folder, [], Opts).

-spec fold(ref(), object_folder(Acc), Acc) -> Acc | no_return().
fold(Reference, Folder, Acc) ->
    fold(Reference, Folder, Acc, undefined).

-spec fold(ref(), object_folder(Acc), Acc, transport_opts()) -> Acc | no_return().
fold(Reference, Folder, Acc, Opts) ->
    Version = ref_to_version(Reference),
    unwrap(dmt_client_cache:fold(Version, Folder, Acc, Opts)).

-spec commit(version(), commit()) -> version() | no_return().
commit(Version, Commit) ->
    commit(Version, Commit, undefined).

-spec commit(version(), commit(), transport_opts()) -> version() | no_return().
commit(Version, Commit, Opts) ->
    dmt_client_backend:commit(Version, Commit, Opts).

-spec get_last_version() -> version().
get_last_version() ->
    dmt_client_cache:get_last_version().

-spec pull_range(version(), limit()) -> history() | no_return().
pull_range(Version, Limit) ->
    pull_range(Version, Limit, undefined).

-spec pull_range(version(), limit(), transport_opts()) -> history() | no_return().
pull_range(Version, Limit, Opts) ->
    dmt_client_backend:pull_range(Version, Limit, Opts).

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
%% DISCUSS: shouldn't version_not_found be handled some other way?
unwrap({error, _}) -> erlang:throw(#'ObjectNotFound'{}).

-spec ref_to_version(ref()) -> version().
ref_to_version({version, Version}) ->
    Version;
ref_to_version({head, #'Head'{}}) ->
    dmt_client_cache:get_last_version().
