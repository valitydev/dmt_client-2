-module(dmt_client_cache).

-behaviour(gen_server).

%% API

-export([start_link/0]).

-export([get/2]).
-export([get_object/3]).
-export([get_objects_by_type/3]).
-export([fold_objects/4]).
-export([get_last_version/0]).
-export([update/0]).

%% gen_server callbacks

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TABLE, ?MODULE).
-define(SERVER, ?MODULE).
-define(DEFAULT_INTERVAL, 5000).
-define(DEFAULT_LIMIT, 10).
-define(DEFAULT_CALL_TIMEOUT, 10000).
-define(DEFAULT_MAX_ELEMENTS, 20).
%% 50Mb by default
-define(DEFAULT_MAX_MEMORY, 52428800).

-include_lib("damsel/include/dmsl_domain_conf_thrift.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(META_TABLE_OPTS, [
    named_table,
    ordered_set,
    public,
    {read_concurrency, true},
    {write_concurrency, true},
    {keypos, #snap.vsn}
]).

-define(SNAPSHOT_TABLE_OPTS, [
    ordered_set,
    protected,
    {read_concurrency, true},
    {write_concurrency, false},
    {keypos, #object.ref}
]).

-type timestamp() :: integer().

-record(snap, {
    vsn :: dmt_client:vsn(),
    tid :: ets:tid(),
    last_access :: timestamp()
}).

-type snap() :: #snap{}.

-record(object, {
    ref :: dmt_client:object_ref() | ets_match(),
    obj :: dmt_client:domain_object() | ets_match()
}).

-type ets_match() :: '_' | '$1' | {atom(), ets_match()}.

-type woody_error() :: {woody_error, woody_error:system_error()}.

-type from() :: {pid(), term()}.
-type fetch_result() ::
    {ok, dmt_client:snapshot()} | {error, version_not_found | woody_error() | {already_fetched, dmt_client:vsn()}}.

-type dispatch_fun() :: fun((from(), fetch_result()) -> any()).
-type waiters() :: #{
    dmt_client:ref() => [{from() | undefined, dispatch_fun()}]
}.

-record(state, {
    timer = undefined :: undefined | reference(),
    waiters = #{} :: waiters(),
    config = #{} :: cache_config()
}).

-type cache_config() :: #{
    max_snapshots => non_neg_integer(),
    max_memory => non_neg_integer()
}.

-type state() :: #state{}.

%%% API

-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec get(dmt_client:vsn(), dmt_client:opts()) ->
    {ok, dmt_client:snapshot()} | {error, version_not_found | woody_error()}.
get(Version, Opts) ->
    case ensure_version(Version, Opts) of
        {ok, Version} ->
            get_cached_snapshot(Version);
        {error, _} = Error ->
            Error
    end.

-spec get_object(dmt_client:vsn(), dmt_client:object_ref(), dmt_client:opts()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found | woody_error()}.
get_object(Version, ObjectRef, Opts) ->
    case ensure_version(Version, Opts) of
        {ok, Version} -> do_get_object(Version, ObjectRef);
        {error, _} = Error -> Error
    end.

-spec get_objects_by_type(dmt_client:vsn(), dmt_client:object_type(), dmt_client:opts()) ->
    {ok, [dmt_client:untagged_domain_object()]} | {error, version_not_found | woody_error()}.
get_objects_by_type(Version, ObjectType, Opts) ->
    case ensure_version(Version, Opts) of
        {ok, Version} -> do_get_objects_by_type(Version, ObjectType);
        {error, _} = Error -> Error
    end.

-spec fold_objects(dmt_client:vsn(), dmt_client:object_folder(Acc), Acc, dmt_client:opts()) ->
    {ok, Acc} | {error, version_not_found | woody_error()}.
fold_objects(Version, Folder, Acc, Opts) ->
    case ensure_version(Version, Opts) of
        {ok, Version} -> do_fold_objects(Version, Folder, Acc);
        {error, _} = Error -> Error
    end.

-spec get_last_version() -> dmt_client:vsn() | no_return().
get_last_version() ->
    UseCached = genlib_app:env(dmt_client, use_cached_last_version, true),
    Result = last_version_in_cache(),
    case {Result, UseCached} of
        {{ok, Version}, true} ->
            Version;
        {{error, version_not_found}, _} ->
            try_update();
        {_, false} ->
            try_update()
    end.

-spec update() -> {ok, dmt_client:vsn()} | {error, woody_error()}.
update() ->
    call(update).

try_update() ->
    case update() of
        {ok, Version} ->
            Version;
        {error, Error} ->
            erlang:error(Error)
    end.

%%% gen_server callbacks

-spec init(_) -> {ok, state(), 0}.
init(_) ->
    ok = create_tables(),
    State = #state{config = build_config()},
    {ok, State, 0}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call({fetch_version, Version, Opts}, From, State) ->
    case ets:member(?TABLE, Version) of
        true ->
            {reply, {ok, Version}, State};
        false ->
            {noreply, fetch_by_reference({version, Version}, From, Opts, State)}
    end;
handle_call(update, From, State) ->
    {noreply, update(From, State)};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({dispatch, Reference, Result}, #state{waiters = Waiters} = State) ->
    _ = [DispatchFun(From, Result) || {From, DispatchFun} <- maps:get(Reference, Waiters, [])],
    {noreply, State#state{waiters = maps:remove(Reference, Waiters)}};
handle_cast(cleanup, State) ->
    cleanup(State),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(timeout, State) ->
    {noreply, update(undefined, State)};
handle_info(_Msg, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions

-spec create_tables() -> ok.
create_tables() ->
    ?TABLE = ets:new(?TABLE, ?META_TABLE_OPTS),
    ok.

build_config() ->
    CacheLimits = genlib_app:env(dmt_client, max_cache_size, #{}),
    MaxElements = genlib_map:get(elements, CacheLimits, ?DEFAULT_MAX_ELEMENTS),
    true = 0 =< MaxElements,
    MaxMemory = genlib_map:get(memory, CacheLimits, ?DEFAULT_MAX_MEMORY),
    true = 0 =< MaxMemory,
    #{
        max_memory => MaxMemory,
        max_snapshots => MaxElements
    }.

-spec call(term()) -> term().
call(Msg) ->
    DefTimeout = application:get_env(dmt_client, cache_server_call_timeout, ?DEFAULT_CALL_TIMEOUT),
    call(Msg, DefTimeout).

-spec call(term(), timeout()) -> term().
call(Msg, Timeout) ->
    try
        gen_server:call(?SERVER, Msg, Timeout)
    catch
        exit:{timeout, {gen_server, call, _}} ->
            woody_error:raise(system, {external, resource_unavailable, <<"dmt_client_cache timeout">>})
    end.

-spec cast(term()) -> ok.
cast(Msg) ->
    gen_server:cast(?SERVER, Msg).

ensure_version(Version, Opts) ->
    case ets:member(?TABLE, Version) of
        true -> {ok, Version};
        false -> call({fetch_version, Version, Opts})
    end.

-spec get_cached_snapshot(dmt_client:vsn()) -> {ok, dmt_client:snapshot()} | {error, version_not_found}.
get_cached_snapshot(Version) ->
    case fetch_snap(Version) of
        {ok, Snap} ->
            build_snapshot(Snap);
        {error, version_not_found} = Error ->
            Error
    end.

-spec do_get_object(dmt_client:vsn(), dmt_client:object_ref()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found}.
do_get_object(Version, ObjectRef) ->
    try
        Snap = get_snap(Version),
        ets:lookup(Snap#snap.tid, ObjectRef)
    of
        [#object{obj = Object}] ->
            {ok, Object};
        [] ->
            {error, object_not_found}
    catch
        % table was deleted
        % DISCUSS(ED-185): is it correct though? Wouldn't recuring back to original function be better?
        % This way, we can fetch wiped snapshot again only for this version
        error:badarg ->
            {error, version_not_found}
    end.

do_get_objects_by_type(Version, ObjectType) ->
    MatchSpec = [
        {{object, '_', {ObjectType, '$1'}}, [], ['$1']}
    ],
    try
        Snap = get_snap(Version),
        ets:select(Snap#snap.tid, MatchSpec)
    of
        Result ->
            {ok, Result}
    catch
        %% DISCUSS: same as above for do_get_object
        error:badarg ->
            {error, version_not_found}
    end.

do_fold_objects(Version, Folder, Acc) ->
    MappedFolder = fun({object, {Type, _Ref}, {Type, Object}}, AccIn) ->
        Folder(Type, Object, AccIn)
    end,
    try
        Snap = get_snap(Version),
        ets:foldl(MappedFolder, Acc, Snap#snap.tid)
    of
        Result ->
            {ok, Result}
    catch
        %% DISCUSS: same as above for do_get_object
        error:badarg ->
            {error, version_not_found}
    end.

-spec put_snapshot(dmt_client:snapshot()) -> ok.
put_snapshot(#domain_conf_Snapshot{version = Version, domain = Domain}) ->
    case fetch_snap(Version) of
        {ok, _Snap} ->
            ok;
        {error, version_not_found} ->
            TID = ets:new(?MODULE, [{heir, whereis(?SERVER), ok}] ++ ?SNAPSHOT_TABLE_OPTS),
            true = put_domain_to_table(TID, Domain),
            Snap = #snap{
                vsn = Version,
                tid = TID,
                last_access = timestamp()
            },
            true = ets:insert(?TABLE, Snap),
            cast(cleanup),
            ok
    end.

-spec put_domain_to_table(ets:tid(), dmt_client:domain()) -> true.
put_domain_to_table(TID, Domain) ->
    dmt_domain:fold(
        fun(Ref, Object, _) ->
            true = ets:insert(TID, #object{ref = Ref, obj = Object})
        end,
        true,
        Domain
    ).

-spec get_snap(dmt_client:vsn()) -> snap() | no_return().
get_snap(Version) ->
    case fetch_snap(Version) of
        {ok, Snap} -> Snap;
        {error, version_not_found} -> error(badarg)
    end.

-spec fetch_snap(dmt_client:vsn()) -> {ok, snap()} | {error, version_not_found}.
fetch_snap(Version) ->
    case ets:lookup(?TABLE, Version) of
        [Snap] ->
            _ = update_last_access(Version),
            {ok, Snap};
        [] ->
            {error, version_not_found}
    end.

-spec get_all_snaps() -> [snap()].
get_all_snaps() ->
    ets:tab2list(?TABLE).

update(From, State) ->
    restart_timer(fetch_by_reference({head, #domain_conf_Head{}}, From, #{}, State)).

fetch_by_reference(Reference, From, Opts, #state{waiters = Waiters} = State) ->
    DispatchFun = fun dispatch_reply/2,
    NewWaiters = maybe_fetch(Reference, From, DispatchFun, Waiters, Opts),
    State#state{waiters = NewWaiters}.

-spec maybe_fetch(dmt_client:ref(), from() | undefined, dispatch_fun(), waiters(), dmt_client:opts()) -> waiters().
maybe_fetch(Reference, ReplyTo, DispatchFun, Waiters, Opts) ->
    Prev =
        case maps:find(Reference, Waiters) of
            error ->
                _Pid = schedule_fetch(Reference, Opts),
                [];
            {ok, List} ->
                List
        end,
    Waiters#{Reference => [{ReplyTo, DispatchFun} | Prev]}.

-spec schedule_fetch(dmt_client:ref(), dmt_client:opts()) -> pid().
schedule_fetch(Reference, Opts) ->
    spawn_link(
        fun() ->
            Result =
                case fetch(Reference, Opts) of
                    {ok, Snapshot} ->
                        put_snapshot(Snapshot),
                        {ok, Snapshot#domain_conf_Snapshot.version};
                    {error, {already_fetched, Version}} ->
                        {ok, Version};
                    {error, _} = Error ->
                        Error
                end,

            cast({dispatch, Reference, Result})
        end
    ).

-spec fetch(dmt_client:ref(), dmt_client:opts()) -> fetch_result().
fetch(Reference, Opts) ->
    try
        do_fetch(Reference, Opts)
    catch
        throw:#domain_conf_VersionNotFound{} ->
            {error, version_not_found};
        error:{woody_error, {_Source, _Class, _Details}} = Error ->
            {error, Error}
    end.

-spec do_fetch(dmt_client:ref(), dmt_client:opts()) ->
    {ok, dmt_client:snapshot()}
    | {error, {already_fetched, dmt_client:vsn()}}
    | no_return().
do_fetch({head, #domain_conf_Head{}}, Opts) ->
    case last_version_in_cache() of
        {ok, OldVersion} ->
            case new_commits_exist(OldVersion, Opts) of
                true ->
                    {ok, Head} = get_cached_snapshot(OldVersion),
                    Limit = genlib_app:env(dmt_client, cache_update_pull_limit, ?DEFAULT_LIMIT),
                    {ok, update_head(Head, Limit, Opts)};
                %% Cached version doesn't fall behind upstream
                false ->
                    {error, {already_fetched, OldVersion}}
            end;
        {error, version_not_found} ->
            {ok, dmt_client_backend:checkout({head, #domain_conf_Head{}}, Opts)}
    end;
do_fetch(Reference, Opts) ->
    {ok, dmt_client_backend:checkout(Reference, Opts)}.

new_commits_exist(OldVersion, Opts) ->
    History = dmt_client_backend:pull_range(OldVersion, _PullLimit = 1, Opts),
    map_size(History) > 0.

update_head(Head, PullLimit, Opts) ->
    FreshHistory = dmt_client_backend:pull_range(Head#domain_conf_Snapshot.version, PullLimit, Opts),
    {ok, NewHead} = dmt_history:head(FreshHistory, Head),

    %% Received history is smaller then PullLimit => reached the top of changes
    case map_size(FreshHistory) < PullLimit of
        true ->
            NewHead;
        false ->
            update_head(NewHead, PullLimit, Opts)
    end.

-spec dispatch_reply(from() | undefined, fetch_result()) -> _.
dispatch_reply(undefined, _Result) ->
    ok;
dispatch_reply(From, Response) ->
    gen_server:reply(From, Response).

-spec build_snapshot(snap()) -> {ok, dmt_client:snapshot()} | {error, version_not_found}.
build_snapshot(#snap{vsn = Version, tid = TID}) ->
    try
        Domain = ets:foldl(
            fun(#object{obj = Object}, Domain) ->
                {ok, NewDomain} = dmt_domain:insert(Object, Domain),
                NewDomain
            end,
            dmt_domain:new(),
            TID
        ),
        {ok, #domain_conf_Snapshot{version = Version, domain = Domain}}
    catch
        % table was deleted due to cleanup process or crash
        error:badarg ->
            {error, version_not_found}
    end.

-spec last_version_in_cache() -> {ok, dmt_client:vsn()} | {error, version_not_found}.
last_version_in_cache() ->
    case ets:last(?TABLE) of
        '$end_of_table' ->
            {error, version_not_found};
        Version ->
            {ok, Version}
    end.

-spec restart_timer(state()) -> state().
restart_timer(State = #state{timer = undefined}) ->
    start_timer(State);
restart_timer(State = #state{timer = TimerRef}) ->
    _ = erlang:cancel_timer(TimerRef),
    start_timer(State#state{timer = undefined}).

-spec start_timer(state()) -> state().
start_timer(State = #state{timer = undefined}) ->
    Interval = genlib_app:env(dmt_client, cache_update_interval, ?DEFAULT_INTERVAL),
    State#state{timer = erlang:send_after(Interval, self(), timeout)}.

-spec cleanup(state()) -> ok.
cleanup(#state{config = Config}) ->
    Snaps = get_all_snaps(),
    Sorted = lists:keysort(#snap.last_access, Snaps),
    {ok, HeadVersion} = last_version_in_cache(),
    cleanup(Sorted, Config, HeadVersion).

-spec cleanup([snap()], cache_config(), dmt_client:vsn()) -> ok.
cleanup([], _Config, _HeadVersion) ->
    ok;
cleanup(Snaps, Config, HeadVersion) ->
    SnapshotCount = get_snapshot_count(),
    MemoryUsage = get_snapshot_memory_usage(),
    MaxSnapshots = maps:get(max_snapshots, Config),
    MaxMemory = maps:get(max_memory, Config),
    case SnapshotCount > MaxSnapshots orelse (SnapshotCount > 1 andalso MemoryUsage > MaxMemory) of
        true ->
            Tail = remove_earliest(Snaps, HeadVersion),
            cleanup(Tail, Config, HeadVersion);
        false ->
            ok
    end.

-spec get_snapshot_count() -> non_neg_integer().
get_snapshot_count() ->
    ets:info(?TABLE, size).

-spec get_snapshot_memory_usage() -> non_neg_integer().
get_snapshot_memory_usage() ->
    WordSize = erlang:system_info(wordsize),
    WordCount =
        ets:foldl(
            fun(#snap{tid = TID}, Words) ->
                Words + ets:info(TID, memory)
            end,
            0,
            ?TABLE
        ),
    WordSize * WordCount.

-spec remove_earliest([snap()], dmt_client:vsn()) -> [snap()].
remove_earliest([#snap{vsn = HeadVersion} | Tail], HeadVersion) ->
    Tail;
remove_earliest([Snap | Tail], _HeadVersion) ->
    remove_snap(Snap),
    Tail.

-spec remove_snap(snap()) -> ok.
remove_snap(#snap{tid = TID, vsn = Version}) ->
    true = ets:delete(?TABLE, Version),
    true = ets:delete(TID),
    ok.

-spec update_last_access(dmt_client:vsn()) -> boolean().
update_last_access(Version) ->
    ets:update_element(?TABLE, Version, {#snap.last_access, timestamp()}).

-spec timestamp() -> timestamp().
timestamp() ->
    erlang:monotonic_time(microsecond).

%%% Unit tests

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

-type testcase() :: function() | {_Loc, function()} | [testcase()] | {setup, function(), testcase()}.

% dirty hack for warn_missing_spec
-spec test() -> any().

-spec all_test_() -> testcase().

all_test_() ->
    {setup,
        fun() ->
            create_tables(),
            %% So that put_snapshot works correctly
            register(?SERVER, self())
        end,
        [
            fun test_cleanup/0,
            fun test_last_access/0,
            fun test_get_object/0,
            fun test_get_object_by_type/0,
            fun test_fold/0
        ]}.

set_cache_limits(Elements) ->
    set_cache_limits(Elements, 52428800).

set_cache_limits(Elements, Memory) ->
    application:set_env(dmt_client, max_cache_size, #{elements => Elements, memory => Memory}).

cleanup() ->
    cleanup(#state{config = build_config()}).

-spec test_cleanup() -> _.
test_cleanup() ->
    set_cache_limits(2),
    ok = put_snapshot(#domain_conf_Snapshot{version = 4, domain = dmt_domain:new()}),
    ok = put_snapshot(#domain_conf_Snapshot{version = 3, domain = dmt_domain:new()}),
    ok = put_snapshot(#domain_conf_Snapshot{version = 2, domain = dmt_domain:new()}),
    ok = put_snapshot(#domain_conf_Snapshot{version = 1, domain = dmt_domain:new()}),
    cleanup(),
    [
        #snap{vsn = 1, _ = _},
        #snap{vsn = 4, _ = _}
    ] = get_all_snaps().

-spec test_last_access() -> _.
test_last_access() ->
    set_cache_limits(3),
    % Tables already created in test_cleanup/0
    ok = put_snapshot(#domain_conf_Snapshot{version = 4, domain = dmt_domain:new()}),
    ok = put_snapshot(#domain_conf_Snapshot{version = 3, domain = dmt_domain:new()}),
    ok = put_snapshot(#domain_conf_Snapshot{version = 2, domain = dmt_domain:new()}),
    Ref = {category, #domain_CategoryRef{id = 1}},
    {error, object_not_found} = get_object(3, Ref, #{}),
    ok = put_snapshot(#domain_conf_Snapshot{version = 1, domain = dmt_domain:new()}),
    cleanup(),
    [
        #snap{vsn = 1, _ = _},
        #snap{vsn = 3, _ = _},
        #snap{vsn = 4, _ = _}
    ] = get_all_snaps().

-spec test_get_object() -> _.
test_get_object() ->
    set_cache_limits(1),
    Version = 5,
    Cat = {_, {_, Ref, _}} = dmt_client_fixtures:fixture(category),
    Domain = dmt_client_fixtures:domain_insert(Cat),

    ok = put_snapshot(#domain_conf_Snapshot{version = Version, domain = Domain}),
    {ok, Cat} = get_object(Version, {category, Ref}, #{}).

-spec test_get_object_by_type() -> _.
test_get_object_by_type() ->
    set_cache_limits(1),
    Version = 6,
    {_, Cat1} = dmt_client_fixtures:fixture(category),
    {_, Cat2} = dmt_client_fixtures:fixture(category2),

    Domain = dmt_client_fixtures:domain_with_all_fixtures(),

    ok = put_snapshot(#domain_conf_Snapshot{version = Version, domain = Domain}),

    {ok, Objects} = get_objects_by_type(Version, category, #{}),
    [Cat1, Cat2] = lists:sort(Objects).

-spec test_fold() -> _.
test_fold() ->
    set_cache_limits(1),
    Version = 7,

    Domain = dmt_client_fixtures:domain_with_all_fixtures(),

    ok = put_snapshot(#domain_conf_Snapshot{version = Version, domain = Domain}),

    {ok, OrdSet} = fold_objects(
        Version,
        fun
            (
                category,
                #'domain_CategoryObject'{
                    ref = #'domain_CategoryRef'{id = ID}
                },
                Acc
            ) ->
                ordsets:add_element(ID, Acc);
            (_Type, _Obj, Acc) ->
                Acc
        end,
        ordsets:new(),
        #{}
    ),

    [1, 2] = ordsets:to_list(OrdSet).

% TEST
-endif.
