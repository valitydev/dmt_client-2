-module(dmt_client_cache).

-behaviour(gen_server).

%% API

-export([start_link/0]).

-export([checkout/2]).
-export([checkout_object/3]).
-export([checkout_objects_by_type/3]).
-export([checkout_fold_objects/4]).
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
-define(DEFAULT_UPDATE_INTERVAL, 5000).
-define(DEFAULT_CLEANUP_INTERVAL, 5000).
-define(DEFAULT_LIMIT, 10).
-define(DEFAULT_CALL_TIMEOUT, 10000).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

-define(meta_table_opts, [
    named_table,
    ordered_set,
    public,
    {read_concurrency, true},
    {write_concurrency, true},
    {keypos, #snap.vsn}
]).

-define(snapshot_table_opts, [
    ordered_set,
    protected,
    {read_concurrency, true},
    {write_concurrency, false},
    {keypos, #object.ref}
]).

-define(USERS_TABLE, dmt_client_cache_users).

-define(users_table_opts, [
    named_table,
    bag,
    public,
    {read_concurrency, false},
    {write_concurrency, true},
    {keypos, #user.vsn}
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

-record(user, {
    vsn :: dmt_client:vsn() | ets_match(),
    requested_at :: timestamp() | ets_match(),
    pid :: pid() | ets_match()
}).

-type ets_match() :: '_' | '$1' | {atom(), ets_match()}.

-type woody_error() :: {woody_error, woody_error:system_error()}.

-type from() :: {pid(), term()}.
-type fetch_result() :: {ok, dmt_client:snapshot()} | {error, version_not_found | woody_error()}.
-type dispatch_fun() :: fun((from(), fetch_result()) -> any()).
-type waiters() :: #{
    dmt_client:ref() => [{from() | undefined, dispatch_fun()}]
}.

-record(state, {
    update_timer = undefined :: undefined | reference(),
    cleanup_timer = undefined :: undefined | reference(),
    waiters = #{} :: waiters()
}).

-type state() :: #state{}.

%%% API

-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec checkout(dmt_client:vsn(), dmt_client:transport_opts()) ->
    {ok, dmt_client:snapshot()} | {error, version_not_found | woody_error()}.
checkout(Version, Opts) ->
    with_version(Version, Opts, fun
        ({ok, _Version}) -> do_checkout(Version);
        ({error, _} = Error) -> Error
    end).

-spec checkout_object(dmt_client:vsn(), dmt_client:object_ref(), dmt_client:transport_opts()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found | woody_error()}.
checkout_object(Version, ObjectRef, Opts) ->
    with_version(Version, Opts, fun
        ({ok, _Version}) -> do_checkout_object(Version, ObjectRef);
        ({error, _} = Error) -> Error
    end).

-spec checkout_objects_by_type(dmt_client:vsn(), dmt_client:object_type(), dmt_client:transport_opts()) ->
    {ok, [dmt_client:domain_object()]} | {error, version_not_found | woody_error()}.
checkout_objects_by_type(Version, ObjectType, Opts) ->
    with_version(Version, Opts, fun
        ({ok, _Version}) -> do_checkout_objects_by_type(Version, ObjectType);
        ({error, _} = Error) -> Error
    end).

-spec checkout_fold_objects(dmt_client:vsn(), dmt_client:object_folder(Acc), Acc, dmt_client:transport_opts()) ->
    {ok, Acc} | {error, version_not_found | woody_error()}.
checkout_fold_objects(Version, Folder, Acc, Opts) ->
    with_version(Version, Opts, fun
        ({ok, _Version}) -> do_checkout_fold_objects(Version, Folder, Acc);
        ({error, _} = Error) -> Error
    end).

-spec get_last_version() -> dmt_client:vsn() | no_return().
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

-spec update() -> {ok, dmt_client:vsn()} | {error, woody_error()}.
update() ->
    case call(update) of
        {fetched, Version} ->
            cast({unlock, Version, self()}),
            {ok, Version};
        AsIs ->
            AsIs
    end.

%%% gen_server callbacks

-spec init(_) -> {ok, state()}.
init(_) ->
    ok = create_tables(),
    self() ! {update_timer, timeout},
    {ok, restart_cleanup_timer(#state{})}.

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
    VersionWaiters = maps:get(Reference, Waiters, []),
    add_users(Reference, VersionWaiters),
    _ = [DispatchFun(From, Result) || {From, DispatchFun} <- VersionWaiters],
    {noreply, State#state{waiters = maps:remove(Reference, Waiters)}};
handle_cast(update, State) ->
    {noreply, update(undefined, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({update_timer, timeout}, State) ->
    {noreply, update(undefined, State)};
handle_info({cleanup_timer, timeout}, State) ->
    cleanup(),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions

%% update op: nothing to do
add_users({head, _}, _Waiters) ->
    ok;
add_users({version, Version}, Waiters) ->
    [ets:insert(?USERS_TABLE, #user{vsn = Version, pid = Pid}) || {{Pid, _}, _} <- Waiters],
    ok.

-spec create_tables() -> ok.
create_tables() ->
    ?TABLE = ets:new(?TABLE, ?meta_table_opts),
    ?USERS_TABLE = ets:new(?USERS_TABLE, ?users_table_opts),
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

with_version(Version, Opts, Fun) ->
    Result =
        case ets:member(?TABLE, Version) of
            true -> {ok, Version};
            false -> call({fetch_version, Version, Opts})
        end,
    case Result of
        {ok, Version} ->
            Fun({ok, Version});
        {fetched, Version} ->
            try
                Fun({ok, Version})
            catch
                Class:Reason:Stacktrace ->
                    erlang:raise(Class, Reason, Stacktrace)
            after
                %% Notify that we finished working on ets copy for further possible cleanup
                unlock_version(Version)
            end;
        Error ->
            Error
    end.

-spec do_checkout(dmt_client:vsn()) -> {ok, dmt_client:snapshot()} | {error, version_not_found}.
do_checkout(Version) ->
    case get_snap(Version) of
        {ok, Snap} ->
            build_snapshot(Snap);
        {error, version_not_found} = Error ->
            Error
    end.

-spec do_checkout_object(dmt_client:vsn(), dmt_client:object_ref()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found}.
do_checkout_object(Version, ObjectRef) ->
    {ok, #snap{tid = TID}} = get_snap(Version),
    try ets:lookup(TID, ObjectRef) of
        [#object{obj = Object}] ->
            {ok, Object};
        [] ->
            {error, object_not_found}
    catch
        % table was deleted
        % DISCUSS: is it correct though? Wouldn't recuring back to original function be better?
        % This way, we can fetch wiped snapshot again only for this version
        error:badarg ->
            {error, version_not_found}
    end.

do_checkout_objects_by_type(Version, ObjectType) ->
    {ok, #snap{tid = TID}} = get_snap(Version),
    MatchSpec = [
        {#object{ref = '_', obj = {ObjectType, '$1'}}, [], ['$1']}
    ],
    try ets:select(TID, MatchSpec) of
        Result ->
            {ok, Result}
    catch
        %% DISCUSS: same as above for do_checkout_object
        error:badarg ->
            {error, version_not_found}
    end.

do_checkout_fold_objects(Version, Folder, Acc) ->
    {ok, #snap{tid = TID}} = get_snap(Version),
    MappedFolder = fun({object, {Type, _Ref}, {Type, Object}}, AccIn) ->
        Folder(Type, Object, AccIn)
    end,
    try ets:foldl(MappedFolder, Acc, TID) of
        Result ->
            {ok, Result}
    catch
        %% DISCUSS: same as above for do_checkout_object
        error:badarg ->
            {error, version_not_found}
    end.

-spec put_snapshot(dmt_client:snapshot()) -> ok.
put_snapshot(#'Snapshot'{version = Version, domain = Domain}) ->
    case get_snap(Version) of
        {ok, _Snap} ->
            ok;
        {error, version_not_found} ->
            TID = ets:new(?MODULE, [{heir, whereis(?SERVER), ok}] ++ ?snapshot_table_opts),
            true = put_domain_to_table(TID, Domain),
            Snap = #snap{
                vsn = Version,
                tid = TID,
                last_access = timestamp()
            },
            true = ets:insert(?TABLE, Snap),
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

-spec get_snap(dmt_client:vsn()) -> {ok, snap()} | {error, version_not_found}.
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

update(From, State) ->
    restart_update_timer(fetch_by_reference({head, #'Head'{}}, From, undefined, State)).

fetch_by_reference(Reference, From, Opts, #state{waiters = Waiters} = State) ->
    DispatchFun = fun dispatch_reply/2,
    NewWaiters = maybe_fetch(Reference, From, DispatchFun, Waiters, Opts),
    State#state{waiters = NewWaiters}.

-spec maybe_fetch(dmt_client:ref(), from() | undefined, dispatch_fun(), waiters(), dmt_client:transport_opts()) ->
    waiters().
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

-spec schedule_fetch(dmt_client:ref(), dmt_client:transport_opts()) -> pid().
schedule_fetch(Reference, Opts) ->
    proc_lib:spawn_link(
        fun() ->
            Result =
                case fetch(Reference, Opts) of
                    {ok, Snapshot} ->
                        put_snapshot(Snapshot),
                        {ok, Snapshot#'Snapshot'.version};
                    {error, _} = Error ->
                        Error
                end,
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
            update_head(OldHead, Limit, Opts);
        {error, version_not_found} ->
            dmt_client_backend:checkout({head, #'Head'{}}, Opts)
    end;
do_fetch(Reference, Opts) ->
    dmt_client_backend:checkout(Reference, Opts).

update_head(Head, PullLimit, Opts) ->
    FreshHistory = dmt_client_backend:pull_range(Head#'Snapshot'.version, PullLimit, Opts),
    {ok, NewHead} = dmt_history:head(FreshHistory, Head),
    %% Received history is smaller then PullLimit => reached the top of changes
    case map_size(FreshHistory) < PullLimit of
        true -> NewHead;
        false -> update_head(NewHead, PullLimit, Opts)
    end.

-spec dispatch_reply(from() | undefined, fetch_result()) -> _.
dispatch_reply(undefined, _Result) ->
    ok;
dispatch_reply(From, {ok, Version}) ->
    gen_server:reply(From, {fetched, Version});
dispatch_reply(From, Error) ->
    gen_server:reply(From, Error).

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
            do_checkout(Version);
        {error, version_not_found} = Error ->
            Error
    end.

-spec do_get_last_version() -> {ok, dmt_client:vsn()} | {error, version_not_found}.
do_get_last_version() ->
    case ets:last(?TABLE) of
        '$end_of_table' ->
            {error, version_not_found};
        Version ->
            {ok, Version}
    end.

-spec restart_update_timer(state()) -> state().
restart_update_timer(State = #state{update_timer = TimerRef}) ->
    cancel_timer(TimerRef),
    Interval = genlib_app:env(dmt_client, cache_update_interval, ?DEFAULT_UPDATE_INTERVAL),
    State#state{update_timer = erlang:send_after(Interval, self(), {update_timer, timeout})}.

-spec restart_cleanup_timer(state()) -> state().
restart_cleanup_timer(State = #state{cleanup_timer = TimerRef}) ->
    cancel_timer(TimerRef),
    Interval = genlib_app:env(dmt_client, cache_update_interval, ?DEFAULT_CLEANUP_INTERVAL),
    State#state{cleanup_timer = erlang:send_after(Interval, self(), {cleanup_timer, timeout})}.

cancel_timer(undefined) ->
    ok;
cancel_timer(TimerRef) ->
    _ = erlang:cancel_timer(TimerRef),
    ok.

unlock_version(Version) ->
    ets:delete_object(?USERS_TABLE, #user{vsn = Version, pid = self()}).

-spec cleanup() -> ok.
cleanup() ->
    Snaps = get_all_snaps(),
    Sorted = lists:keysort(#snap.last_access, Snaps),
    case do_get_last_version() of
        {ok, HeadVersion} -> cleanup(Sorted, HeadVersion);
        _ -> ok
    end.

-spec cleanup([snap()], dmt_client:vsn()) -> ok.
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

-spec remove_earliest([snap()], dmt_client:vsn()) -> [snap()].
remove_earliest([#snap{vsn = HeadVersion} | Tail], HeadVersion) ->
    Tail;
remove_earliest([Snap = #snap{vsn = Version} | Tail], HeadVersion) ->
    case ets:select_count(?USERS_TABLE, [{#user{vsn = Version, pid = '_'}, [], ['$_']}]) of
        0 ->
            remove_snap(Snap),
            Tail;
        _ ->
            [Snap | remove_earliest(Tail, HeadVersion)]
    end.

-spec remove_snap(snap()) -> ok.
remove_snap(#snap{tid = TID, vsn = Version}) ->
    true = ets:delete(?TABLE, Version),
    _ = ets:select_delete(?USERS_TABLE, [{#user{vsn = Version, pid = '_'}, [], ['$_']}]),
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

% dirty hack for warn_missing_spec
-spec test() -> any().

-spec all_test_() -> ok.

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

-spec test_cleanup() -> _.
test_cleanup() ->
    set_cache_limits(2),
    ok = put_snapshot(#'Snapshot'{version = 4, domain = dmt_domain:new()}),
    ok = put_snapshot(#'Snapshot'{version = 3, domain = dmt_domain:new()}),
    ok = put_snapshot(#'Snapshot'{version = 2, domain = dmt_domain:new()}),
    ok = put_snapshot(#'Snapshot'{version = 1, domain = dmt_domain:new()}),
    cleanup(),
    [
        #snap{vsn = 1, _ = _},
        #snap{vsn = 4, _ = _}
    ] = get_all_snaps().

-spec test_last_access() -> _.
test_last_access() ->
    set_cache_limits(3),
    % Tables already created in test_cleanup/0
    ok = put_snapshot(#'Snapshot'{version = 4, domain = dmt_domain:new()}),
    ok = put_snapshot(#'Snapshot'{version = 3, domain = dmt_domain:new()}),
    ok = put_snapshot(#'Snapshot'{version = 2, domain = dmt_domain:new()}),
    Ref = {category, #'domain_CategoryRef'{id = 1}},
    {error, object_not_found} = checkout_object(3, Ref, undefined),
    ok = put_snapshot(#'Snapshot'{version = 1, domain = dmt_domain:new()}),
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
    Cat = {_, Ref, _} = fixture(category),
    Domain = dmt_insert_many(
        [{category, Cat}]
    ),

    ok = put_snapshot(#'Snapshot'{version = Version, domain = Domain}),

    {ok, {category, Cat}} = checkout_object(Version, {category, Ref}, undefined).

-spec test_get_object_by_type() -> _.
test_get_object_by_type() ->
    set_cache_limits(1),
    Version = 6,
    Cat1 = fixture(category),
    Cat2 = fixture(category_2),

    Domain = domain_with_all_fixtures(),

    ok = put_snapshot(#'Snapshot'{version = Version, domain = Domain}),

    {ok, Objects} = checkout_objects_by_type(Version, category, undefined),
    [Cat1, Cat2] = lists:sort(Objects).

-spec test_fold() -> _.
test_fold() ->
    set_cache_limits(1),
    Version = 7,

    Domain = domain_with_all_fixtures(),

    ok = put_snapshot(#'Snapshot'{version = Version, domain = Domain}),

    {ok, OrdSet} = checkout_fold_objects(
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
        undefined
    ),

    [1, 2] = ordsets:to_list(OrdSet).

domain_with_all_fixtures() ->
    dmt_insert_many(
        [
            {category, fixture(category)},
            {category, fixture(category_2)},
            {currency, fixture(currency)}
        ]
    ).

dmt_insert_many(Objects) ->
    dmt_insert_many(Objects, dmt_domain:new()).

dmt_insert_many(Objects, Domain) ->
    lists:foldl(
        fun(Object, DomainIn) ->
            {ok, DomainOut} = dmt_domain:insert(Object, DomainIn),
            DomainOut
        end,
        Domain,
        Objects
    ).

fixture(ID) ->
    maps:get(
        ID,
        #{
            category => #'domain_CategoryObject'{
                ref = #'domain_CategoryRef'{id = 1},
                data = #'domain_Category'{
                    name = <<"cat">>,
                    description = <<"Sample category">>
                }
            },
            category_2 => #'domain_CategoryObject'{
                ref = #'domain_CategoryRef'{id = 2},
                data = #'domain_Category'{
                    name = <<"dog">>,
                    description = <<"Sample category">>
                }
            },
            currency => #'domain_CurrencyObject'{
                ref = #'domain_CurrencyRef'{symbolic_code = <<"USD">>},
                data = #'domain_Currency'{
                    name = <<"dog">>,
                    symbolic_code = <<"USD">>,
                    numeric_code = 840,
                    exponent = 2
                }
            }
        }
    ).

% TEST
-endif.
