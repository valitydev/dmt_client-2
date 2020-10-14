-module(dmt_client_cache).

-behaviour(gen_server).

%% API

-export([start_link/0]).

-export([get/1]).
-export([get/2]).
-export([get_last_version/0]).
-export([get_object/2]).
-export([get_object/3]).
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

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(meta_table_opts, [
    named_table,
    ordered_set,
    public,
    {read_concurrency, true},
    {write_concurrency, true},
    {keypos, #snap.vsn}
]).

-define(snapshot_table_opts, [
    set,
    protected,
    {read_concurrency, true},
    {write_concurrency, false},
    {keypos, #object.ref}
]).

-type timestamp() :: integer().

-record(snap, {
    vsn :: dmt_client:version(),
    tid :: ets:tid(),
    last_access :: timestamp()
}).

-type snap() :: #snap{}.

-record(object, {
    ref :: dmt_client:object_ref(),
    obj :: dmt_client:domain_object()
}).

-type woody_error() :: {woody_error, woody_error:system_error()}.

-type from() :: {pid(), term()}.
-type fetch_result() :: {ok, dmt_client:snapshot()} | {error, version_not_found | woody_error()}.
-type dispatch_fun() :: fun((from(), fetch_result()) -> any()).
-type waiters() :: #{
    dmt_client:ref() => [{from() | undefined, dispatch_fun()}]
}.

-record(state, {
    timer = undefined :: undefined | reference(),
    waiters = #{} :: waiters()
}).

-type state() :: #state{}.

%%% API

-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec get(dmt_client:version()) -> {ok, dmt_client:snapshot()} | {error, version_not_found | woody_error()}.
get(Version) ->
    get(Version, undefined).

-spec get(dmt_client:version(), dmt_client:transport_opts()) ->
    {ok, dmt_client:snapshot()} | {error, version_not_found | woody_error()}.
get(Version, Opts) ->
    case get_snapshot(Version) of
        {ok, _Snapshot} = Result ->
            Result;
        {error, version_not_found} ->
            call({get_snapshot, Version, Opts})
    end.

-spec get_object(dmt_client:version(), dmt_client:object_ref()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found | woody_error()}.
get_object(Version, ObjectRef) ->
    get_object(Version, ObjectRef, undefined).

-spec get_object(dmt_client:version(), dmt_client:object_ref(), dmt_client:transport_opts()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found | woody_error()}.
get_object(Version, ObjectRef, Opts) ->
    case do_get_object(Version, ObjectRef) of
        {ok, _Object} = Result ->
            Result;
        {error, version_not_found} ->
            call({get_object, Version, ObjectRef, Opts});
        {error, object_not_found} = NotFound ->
            NotFound
    end.

-spec get_last_version() -> dmt_client:version() | no_return().
get_last_version() ->
    case do_get_last_version() of
        {ok, Version} ->
            Version;
        {error, version_not_found} ->
            case update() of
                {ok, Version} ->
                    Version;
                {error, Error} ->
                    erlang:error(Error)
            end
    end.

-spec update() -> {ok, dmt_client:version()} | {error, woody_error()}.
update() ->
    call(update).

%%% gen_server callbacks

-spec init(_) -> {ok, state(), 0}.
init(_) ->
    ok = create_tables(),
    {ok, #state{}, 0}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call({get_object, Version, ObjectRef, Opts}, From, #state{waiters = Waiters} = State) ->
    case do_get_object(Version, ObjectRef) of
        {ok, _Object} = Result ->
            {reply, Result, State};
        {error, object_not_found} = NotFound ->
            {reply, NotFound, State};
        {error, version_not_found} ->
            DispatchFun = dispatch_object(ObjectRef),
            NewWaiters = maybe_fetch({version, Version}, From, DispatchFun, Waiters, Opts),
            {noreply, State#state{waiters = NewWaiters}}
    end;
handle_call(update, From, State) ->
    update_cache(From, State);
handle_call({get_snapshot, Version, Opts}, From, #state{waiters = Waiters} = State) ->
    case get_snapshot(Version) of
        {ok, _Snapshot} = Result ->
            {reply, Result, State};
        {error, version_not_found} ->
            DispatchFun = fun dispatch_snapshot/2,
            NewWaiters = maybe_fetch({version, Version}, From, DispatchFun, Waiters, Opts),
            {noreply, State#state{waiters = NewWaiters}}
    end;
handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({dispatch, Reference, Result}, #state{waiters = Waiters} = State) ->
    case Result of
        {ok, Snapshot} ->
            put_snapshot(Snapshot);
        _ ->
            ok
    end,
    _ = [DispatchFun(From, Result) || {From, DispatchFun} <- maps:get(Reference, Waiters, [])],
    {noreply, State#state{waiters = maps:remove(Reference, Waiters)}};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(timeout, State) ->
    update_cache(undefined, State);
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
    ?TABLE = ets:new(?TABLE, ?meta_table_opts),
    ok.

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

-spec put_snapshot(dmt_client:snapshot()) -> ok.
put_snapshot(#'Snapshot'{version = Version, domain = Domain}) ->
    case get_snap(Version) of
        {ok, _Snap} ->
            ok;
        {error, version_not_found} ->
            TID = ets:new(?MODULE, ?snapshot_table_opts),
            true = put_domain_to_table(TID, Domain),
            Snap = #snap{
                vsn = Version,
                tid = TID,
                last_access = timestamp()
            },
            true = ets:insert(?TABLE, Snap),
            cleanup()
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

-spec get_snapshot(dmt_client:version()) -> {ok, dmt_client:snapshot()} | {error, version_not_found}.
get_snapshot(Version) ->
    case get_snap(Version) of
        {ok, Snap} ->
            do_get_snapshot(Snap);
        {error, version_not_found} = Error ->
            Error
    end.

-spec get_snap(dmt_client:version()) -> {ok, snap()} | {error, version_not_found}.
get_snap(Version) ->
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

-spec do_get_object(dmt_client:version(), dmt_client:object_ref()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found}.
do_get_object(Version, ObjectRef) ->
    case get_snap(Version) of
        {ok, Snap} ->
            get_object_by_snap(Snap, ObjectRef);
        {error, version_not_found} = Error ->
            Error
    end.

-spec get_object_by_snap(snap(), dmt_client:object_ref()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found}.
get_object_by_snap(#snap{tid = TID}, ObjectRef) ->
    try ets:lookup(TID, ObjectRef) of
        [#object{obj = Object}] ->
            {ok, Object};
        [] ->
            {error, object_not_found}
    catch
        % table was deleted
        error:badarg ->
            {error, version_not_found}
    end.

-spec get_object_from_snapshot(dmt_client:object_ref(), dmt_client:snapshot()) ->
    {ok, dmt_client:domain_object()} | {error, object_not_found}.
get_object_from_snapshot(ObjectRef, #'Snapshot'{domain = Domain}) ->
    case dmt_domain:get_object(ObjectRef, Domain) of
        {ok, _Object} = Result ->
            Result;
        error ->
            {error, object_not_found}
    end.

-spec update_cache(from() | undefined, state()) -> {noreply, state()}.
update_cache(From, #state{waiters = Waiters} = State) ->
    DispatchFun = fun dispatch_update/2,
    NewWaiters = maybe_fetch({head, #'Head'{}}, From, DispatchFun, Waiters, undefined),
    {noreply, restart_timer(State#state{waiters = NewWaiters})}.

-spec maybe_fetch(dmt_client:ref(), from() | undefined, dispatch_fun(), waiters(), dmt_client:transport_opts()) ->
    waiters().
maybe_fetch(Reference, From, DispatchFun, Waiters, Opts) ->
    Prev =
        case maps:find(Reference, Waiters) of
            error ->
                _Pid = schedule_fetch(Reference, Opts),
                [];
            {ok, List} ->
                List
        end,
    Waiters#{Reference => [{From, DispatchFun} | Prev]}.

-spec schedule_fetch(dmt_client:ref(), dmt_client:transport_opts()) -> pid().
schedule_fetch(Reference, Opts) ->
    proc_lib:spawn_link(
        fun() ->
            Result = fetch(Reference, Opts),
            cast({dispatch, Reference, Result})
        end
    ).

-spec fetch(dmt_client:ref(), dmt_client:transport_opts()) -> fetch_result().
fetch(Reference, Opts) ->
    try
        Snapshot = do_fetch(Reference, Opts),
        {ok, Snapshot}
    catch
        throw:#'VersionNotFound'{} ->
            {error, version_not_found};
        error:{woody_error, {_Source, _Class, _Details}} = Error ->
            {error, Error}
    end.

-spec do_fetch(dmt_client:ref(), dmt_client:transport_opts()) -> dmt_client:snapshot() | no_return().
do_fetch({head, #'Head'{}}, Opts) ->
    case latest_snapshot() of
        {ok, OldHead} ->
            Limit = genlib_app:env(dmt_client, cache_update_pull_limit, ?DEFAULT_LIMIT),
            FreshHistory = dmt_client_backend:pull_range(OldHead#'Snapshot'.version, Limit, Opts),
            {ok, Head} = dmt_history:head(FreshHistory, OldHead),
            Head;
        {error, version_not_found} ->
            dmt_client_backend:checkout({head, #'Head'{}}, Opts)
    end;
do_fetch(Reference, Opts) ->
    dmt_client_backend:checkout(Reference, Opts).

-spec dispatch_snapshot(from() | undefined, fetch_result()) -> ok.
dispatch_snapshot(undefined, _Result) ->
    ok;
dispatch_snapshot(From, Result) ->
    _ = gen_server:reply(From, Result),
    ok.

-spec dispatch_object(dmt_client:object_ref()) -> dispatch_fun().
dispatch_object(ObjectRef) ->
    fun(From, Result) ->
        Reply =
            case Result of
                {ok, Snapshot} ->
                    get_object_from_snapshot(ObjectRef, Snapshot);
                Error ->
                    Error
            end,
        gen_server:reply(From, Reply)
    end.

-spec dispatch_update(from() | undefined, fetch_result()) -> ok.
dispatch_update(undefined, _Result) ->
    ok;
dispatch_update(From, {ok, #'Snapshot'{version = Version}}) ->
    _ = gen_server:reply(From, {ok, Version}),
    ok;
dispatch_update(From, Error) ->
    _ = gen_server:reply(From, Error),
    ok.

-spec do_get_snapshot(snap()) -> {ok, dmt_client:snapshot()} | {error, version_not_found}.
do_get_snapshot(#snap{vsn = Version, tid = TID}) ->
    try
        Domain = ets:foldl(
            fun(#object{obj = Object}, Domain) ->
                {ok, NewDomain} = dmt_domain:insert(Object, Domain),
                NewDomain
            end,
            dmt_domain:new(),
            TID
        ),
        {ok, #'Snapshot'{version = Version, domain = Domain}}
    catch
        % table was deleted due to cleanup process or crash
        error:badarg ->
            {error, version_not_found}
    end.

-spec latest_snapshot() -> {ok, dmt_client:snapshot()} | {error, version_not_found}.
latest_snapshot() ->
    case do_get_last_version() of
        {ok, Version} ->
            get_snapshot(Version);
        {error, version_not_found} = Error ->
            Error
    end.

-spec do_get_last_version() -> {ok, dmt_client:version()} | {error, version_not_found}.
do_get_last_version() ->
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

-spec cleanup() -> ok.
cleanup() ->
    Snaps = get_all_snaps(),
    Sorted = lists:keysort(#snap.last_access, Snaps),
    {ok, HeadVersion} = do_get_last_version(),
    cleanup(Sorted, HeadVersion).

-spec cleanup([snap()], dmt_client:version()) -> ok.
cleanup([], _HeadVersion) ->
    ok;
cleanup(Snaps, HeadVersion) ->
    {Elements, Memory} = get_cache_size(),
    CacheLimits = genlib_app:env(dmt_client, max_cache_size),
    MaxElements = genlib_map:get(elements, CacheLimits, 20),
    % 50Mb by default
    MaxMemory = genlib_map:get(memory, CacheLimits, 52428800),
    case Elements > MaxElements orelse (Elements > 1 andalso Memory > MaxMemory) of
        true ->
            Tail = remove_earliest(Snaps, HeadVersion),
            cleanup(Tail, HeadVersion);
        false ->
            ok
    end.

-spec get_cache_size() -> {non_neg_integer(), non_neg_integer()}.
get_cache_size() ->
    WordSize = erlang:system_info(wordsize),
    Info = ets:info(?TABLE),
    Words = get_snapshot_tables_size(),
    {proplists:get_value(size, Info), WordSize * Words}.

-spec get_snapshot_tables_size() -> non_neg_integer().
get_snapshot_tables_size() ->
    ets:foldl(
        fun(#snap{tid = TID}, Words) ->
            Words + ets_memory(TID)
        end,
        0,
        ?TABLE
    ).

-spec ets_memory(ets:tid()) -> non_neg_integer().
ets_memory(TID) ->
    Info = ets:info(TID),
    proplists:get_value(memory, Info).

-spec remove_earliest([snap()], dmt_client:version()) -> [snap()].
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

-spec update_last_access(dmt_client:version()) -> boolean().
update_last_access(Version) ->
    ets:update_element(?TABLE, Version, {#snap.last_access, timestamp()}).

-spec timestamp() -> timestamp().
timestamp() ->
    erlang:monotonic_time(microsecond).

%%% Unit tests

-ifdef(TEST).

% dirty hack for warn_missing_spec
-spec test() -> any().

-include_lib("eunit/include/eunit.hrl").

-spec cleanup_test() -> ok.
cleanup_test() ->
    application:set_env(dmt_client, max_cache_size, #{elements => 2, memory => 52428800}),
    ok = create_tables(),
    ok = put_snapshot(#'Snapshot'{version = 4, domain = dmt_domain:new()}),
    ok = timer:sleep(1),
    ok = put_snapshot(#'Snapshot'{version = 3, domain = dmt_domain:new()}),
    ok = timer:sleep(1),
    ok = put_snapshot(#'Snapshot'{version = 2, domain = dmt_domain:new()}),
    ok = timer:sleep(1),
    ok = put_snapshot(#'Snapshot'{version = 1, domain = dmt_domain:new()}),
    [
        #snap{vsn = 1, _ = _},
        #snap{vsn = 4, _ = _}
    ] = get_all_snaps(),
    ok.

-spec last_access_test() -> ok.
last_access_test() ->
    application:set_env(dmt_client, max_cache_size, #{elements => 3, memory => 52428800}),
    % Tables already created in cleanup_test/0
    ok = put_snapshot(#'Snapshot'{version = 4, domain = dmt_domain:new()}),
    ok = timer:sleep(1),
    ok = put_snapshot(#'Snapshot'{version = 3, domain = dmt_domain:new()}),
    ok = timer:sleep(1),
    ok = put_snapshot(#'Snapshot'{version = 2, domain = dmt_domain:new()}),
    ok = timer:sleep(1),
    Ref = {category, #'domain_CategoryRef'{id = 1}},
    {error, object_not_found} = get_object(3, Ref),
    ok = put_snapshot(#'Snapshot'{version = 1, domain = dmt_domain:new()}),
    [
        #snap{vsn = 1, _ = _},
        #snap{vsn = 3, _ = _},
        #snap{vsn = 4, _ = _}
    ] = get_all_snaps(),
    ok.

% TEST
-endif.
