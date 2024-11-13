-module(dmt_client_cache).

-behaviour(gen_server).

%% API

-export([start_link/0]).

-export([get_object/3]).
-export([do_get_object/2]).

%% gen_server callbacks

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TABLE, ?MODULE).
-define(SERVER, ?MODULE).
-define(DEFAULT_CALL_TIMEOUT, 10000).
-define(DEFAULT_MAX_ELEMENTS, 20).
%% 50Mb by default
-define(DEFAULT_MAX_MEMORY, 52428800).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(TABLE_OPTS, [
    named_table,
    ordered_set,
    public,
    {read_concurrency, true},
    {write_concurrency, true},
    {keypos, #object.id}
]).

-type timestamp() :: integer().

-record(object, {
    id :: {dmt_client:object_ref(), dmt_client:vsn()},
    vsn :: dmt_client:vsn(),
    ref :: dmt_client:object_ref() | ets_match(),
    obj :: dmt_client:versioned_object() | ets_match(),
    created_at :: dmt_client:vsn_created_at(),
    last_access :: timestamp()
}).

-type object() :: #object{}.

-type ets_match() :: '_' | '$1' | {atom(), ets_match()}.

-type woody_error() :: {woody_error, woody_error:system_error()}.

-type from() :: {pid(), term()}.
-type fetch_result() ::
    {ok, {dmt_client:base_version(), dmt_client:object_ref()}}
    | {error, version_not_found | woody_error() | {already_fetched, dmt_client:vsn()}}.

-type dispatch_fun() :: fun((from(), fetch_result()) -> any()).
-type waiters() :: #{
    {dmt_client:object_ref(), dmt_client:vsn()} => [{from() | undefined, dispatch_fun()}]
}.

-record(state, {
    waiters = #{} :: waiters(),
    config = #{} :: cache_config()
}).

-type cache_config() :: #{
    max_objects => non_neg_integer(),
    max_memory => non_neg_integer()
}.

-type state() :: #state{}.

%%% API

-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec get_object(dmt_client:object_ref(), dmt_client:vsn(), dmt_client:opts()) ->
    {ok, dmt_client:versioned_object()}
    | {error, version_not_found | object_not_found | woody_error()}.
get_object(ObjectRef, Version, Opts) ->
    case ensure_object_version(ObjectRef, Version, Opts) of
        {ok, {ObjectRef, NewVersion}} -> do_get_object(ObjectRef, NewVersion);
        {error, _} = Error -> Error
    end.

%%% gen_server callbacks

-spec init(_) -> {ok, state(), 0}.
init(_) ->
    ok = create_tables(),
    State = #state{config = build_config()},
    {ok, State, 0}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call({fetch_object_version, ObjectRef, Version, Opts}, From, State) ->
    % Check if object appeared between call and handle
    case ets:member(?TABLE, {ObjectRef, Version}) of
        true ->
            {reply, {ok, {ObjectRef, Version}}, State};
        false ->
            {noreply, fetch_by_reference(ObjectRef, Version, From, Opts, State)}
    end;
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
    ?TABLE = ets:new(?TABLE, ?TABLE_OPTS),
    ok.

build_config() ->
    CacheLimits = genlib_app:env(dmt_client, max_cache_size, #{}),
    MaxElements = genlib_map:get(elements, CacheLimits, ?DEFAULT_MAX_ELEMENTS),
    true = 0 =< MaxElements,
    MaxMemory = genlib_map:get(memory, CacheLimits, ?DEFAULT_MAX_MEMORY),
    true = 0 =< MaxMemory,
    #{
        max_memory => MaxMemory,
        max_objects => MaxElements
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
            woody_error:raise(
                system, {external, resource_unavailable, <<"dmt_client_cache timeout">>}
            )
    end.

-spec cast(term()) -> ok.
cast(Msg) ->
    gen_server:cast(?SERVER, Msg).

ensure_object_version(ObjectRef, #domain_conf_v2_Head{}, Opts) ->
    call({fetch_object_version, ObjectRef, #domain_conf_v2_Head{}, Opts});
ensure_object_version(ObjectRef, Version, Opts) ->
    case ets:member(?TABLE, {ObjectRef, Version}) of
        true ->
            {ok, {ObjectRef, Version}};
        false ->
            call({fetch_object_version, ObjectRef, Version, Opts})
    end.

-spec do_get_object(dmt_client:object_ref(), dmt_client:vsn()) ->
    {ok, dmt_client:versioned_object()} | {error, version_not_found | object_not_found}.
do_get_object(ObjectRef, Version) ->
    case ets:lookup(?TABLE, {ObjectRef, Version}) of
        [#object{obj = Object}] ->
            true = update_last_access(ObjectRef, Version),
            {ok, Object};
        [] ->
            {error, object_not_found}
    end.

put_object_into_table(Ref, Version, Object, CreatedAt) ->
    true = ets:insert(?TABLE, #object{
        id = {Ref, Version},
        ref = Ref,
        vsn = Version,
        obj = Object,
        created_at = CreatedAt,
        last_access = timestamp()
    }).

-spec get_all_objects() -> [object()].
get_all_objects() ->
    ets:tab2list(?TABLE).

fetch_by_reference(ObjectRef, VersionReference, From, Opts, #state{waiters = Waiters} = State) ->
    DispatchFun = fun dispatch_reply/2,
    NewWaiters = maybe_fetch(ObjectRef, VersionReference, From, DispatchFun, Waiters, Opts),
    State#state{waiters = NewWaiters}.

maybe_fetch(ObjectRef, VersionReference, ReplyTo, DispatchFun, Waiters, Opts) ->
    % First check if we already have waiters for this request
    WaiterKey = {ObjectRef, VersionReference},
    case maps:find(WaiterKey, Waiters) of
        {ok, List} ->
            % Request already in progress, just add to waiters
            Waiters#{WaiterKey => [{ReplyTo, DispatchFun} | List]};
        error ->
            % Double check the cache before scheduling a fetch
            case ets:member(?TABLE, {ObjectRef, VersionReference}) of
                true ->
                    % Object appeared in cache while we were processing
                    Waiters;
                false ->
                    % Schedule new fetch and create waiters list
                    _Pid = schedule_fetch(ObjectRef, VersionReference, Opts),
                    Waiters#{WaiterKey => [{ReplyTo, DispatchFun}]}
            end
    end.

schedule_fetch(ObjectRef, VersionReference, Opts) ->
    spawn_link(
        fun() ->
            Result =
                case fetch(ObjectRef, VersionReference, Opts) of
                    #domain_conf_v2_VersionedObject{
                        global_version = Version0,
                        created_at = CreatedAt
                    } = Object ->
                        Version1 = {version, Version0},
                        put_object_into_table(ObjectRef, Version1, Object, CreatedAt),
                        %% This will be called every time some new object is required.
                        %% Maybe consider alternative
                        cast(cleanup),
                        {ok, {ObjectRef, Version1}};
                    {error, _} = Error ->
                        Error
                end,

            cast({dispatch, {ObjectRef, VersionReference}, Result})
        end
    ).

fetch(ObjectRef, VersionReference, Opts) ->
    try
        dmt_client_backend:checkout_object(ObjectRef, VersionReference, Opts)
    catch
        throw:#domain_conf_v2_VersionNotFound{} ->
            {error, version_not_found};
        throw:#domain_conf_v2_ObjectNotFound{} ->
            {error, object_not_found};
        error:{woody_error, {_Source, _Class, _Details}} = Error ->
            {error, Error}
    end.

-spec dispatch_reply(from() | undefined, fetch_result()) -> _.
dispatch_reply(undefined, _Result) ->
    ok;
dispatch_reply(From, Response) ->
    gen_server:reply(From, Response).

-spec cleanup(state()) -> ok.
cleanup(#state{config = Config}) ->
    Objs = get_all_objects(),
    Sorted = lists:keysort(#object.last_access, Objs),
    cleanup(Sorted, Config).

cleanup([], _Config) ->
    ok;
cleanup([Head | Rest], Config) ->
    ObjectCount = get_objects_count(),
    MemoryUsage = get_objects_memory_usage(),
    MaxObjects = maps:get(max_objects, Config),
    MaxMemory = maps:get(max_memory, Config),
    case ObjectCount > MaxObjects orelse (ObjectCount > 1 andalso MemoryUsage > MaxMemory) of
        true ->
            ok = remove_obj(Head),
            cleanup(Rest, Config);
        false ->
            ok
    end.

-spec get_objects_count() -> non_neg_integer().
get_objects_count() ->
    ets:info(?TABLE, size).

-spec get_objects_memory_usage() -> non_neg_integer().
get_objects_memory_usage() ->
    WordSize = erlang:system_info(wordsize),
    WordCount = ets:info(?TABLE, memory),
    WordSize * WordCount.

-spec remove_obj(object()) -> ok.
remove_obj(#object{ref = Ref, vsn = Version}) ->
    true = ets:delete(?TABLE, {Ref, Version}),
    ok.

-spec update_last_access(dmt_client:object_ref(), dmt_client:vsn()) -> boolean().
update_last_access(Reference, Version) ->
    ets:update_element(?TABLE, {Reference, Version}, {#object.last_access, timestamp()}).

-spec timestamp() -> timestamp().
timestamp() ->
    erlang:monotonic_time(microsecond).

%% TODO remake them

%%% Unit tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

%%
%% Test Fixtures
%%

-define(TEST_REF, {test_type, <<"test_id">>}).
-define(TEST_OBJ(Id), #{name => <<"test">>, id => Id}).
-define(TEST_VERSIONED_OBJ(Ver), #domain_conf_v2_VersionedObject{
    global_version = Ver,
    object = ?TEST_OBJ(Ver),
    created_at = <<"2024-01-01T00:00:00Z">>
}).

-dialyzer({nowarn_function, dmt_cache_test_/0}).
-dialyzer({nowarn_function, test_setup/0}).
-dialyzer({nowarn_function, test_cleanup/1}).
-dialyzer({nowarn_function, test_basic_caching/0}).
-dialyzer({nowarn_function, test_size_limits/0}).
-dialyzer({nowarn_function, test_last_access/0}).
-dialyzer({nowarn_function, test_missing_object/0}).
-dialyzer({nowarn_function, test_concurrent_access/0}).

%%
%% Test Cases
%%

-spec dmt_cache_test_() -> _.
dmt_cache_test_() ->
    {foreach, fun test_setup/0, fun test_cleanup/1, [
        {"Basic object caching works", fun test_basic_caching/0},
        {"Cache respects size limits", fun test_size_limits/0},
        {"Cache updates last access time", fun test_last_access/0},
        {"Cache handles missing objects", fun test_missing_object/0},
        {"Cache handles concurrent access", fun test_concurrent_access/0}
    ]}.

%%
%% Setup/Cleanup
%%

test_setup() ->
    application:set_env(dmt_client, max_cache_size, #{
        elements => 2,
        memory => 52428800
    }),
    {ok, Pid} = dmt_client_cache:start_link(),
    Pid.

test_cleanup(Pid) ->
    ets:delete(?TABLE),
    gen_server:stop(Pid),
    meck:unload(),
    application:unset_env(dmt_client, max_cache_size),
    ok.

%%
%% Individual Test Cases
%%

test_basic_caching() ->
    Version = {version, 1},
    meck:new(dmt_client_backend, [passthrough]),
    meck:expect(
        dmt_client_backend,
        checkout_object,
        fun(ObjRef, VersionRef, _Opts) ->
            ?assertEqual(Version, VersionRef),
            ?assertEqual(?TEST_REF, ObjRef),
            ?TEST_VERSIONED_OBJ(1)
        end
    ),

    % First access should fetch from backend
    {ok, Object} = dmt_client_cache:get_object(?TEST_REF, Version, #{}),
    ?assertEqual(?TEST_VERSIONED_OBJ(1), Object),

    % Second access should come from cache
    {ok, CachedObject} = dmt_client_cache:get_object(?TEST_REF, Version, #{}),
    ?assertEqual(Object, CachedObject),

    % Verify backend was called only once
    ?assertEqual(1, meck:num_calls(dmt_client_backend, checkout_object, '_')).

test_size_limits() ->
    meck:new(dmt_client_backend, [passthrough]),
    meck:expect(
        dmt_client_backend,
        checkout_object,
        fun(_ObjRef, {version, Ver}, _Opts) ->
            ?TEST_VERSIONED_OBJ(Ver)
        end
    ),

    % Add three objects to trigger cleanup
    [dmt_client_cache:get_object(?TEST_REF, {version, Ver}, #{}) || Ver <- lists:seq(1, 3)],

    % Verify only newest objects remain
    Objects = ets:tab2list(?TABLE),
    ?assertEqual(2, length(Objects)),

    % Verify the oldest version was evicted
    Result = do_get_object(?TEST_REF, {version, 1}),
    ?assertEqual({error, object_not_found}, Result).

test_last_access() ->
    meck:new(dmt_client_backend, [passthrough]),
    meck:expect(
        dmt_client_backend,
        checkout_object,
        fun(_ObjRef, {version, Ver}, _Opts) ->
            ?TEST_VERSIONED_OBJ(Ver)
        end
    ),

    % Access objects in specific order
    Version1 = {version, 1},
    Version2 = {version, 2},

    {ok, _} = dmt_client_cache:get_object(?TEST_REF, Version1, #{}),
    timer:sleep(100),
    {ok, _} = dmt_client_cache:get_object(?TEST_REF, Version2, #{}),
    timer:sleep(100),
    {ok, _} = dmt_client_cache:get_object(?TEST_REF, Version1, #{}),

    % Get objects sorted by last access
    Objects = lists:keysort(#object.last_access, ets:tab2list(?TABLE)),
    [Obj1, Obj2] = Objects,

    % Version2 should be least recently accessed
    ?assertEqual(Version2, Obj1#object.vsn),
    ?assertEqual(Version1, Obj2#object.vsn).

test_missing_object() ->
    meck:new(dmt_client_backend, [passthrough]),
    meck:expect(
        dmt_client_backend,
        checkout_object,
        fun(_ObjRef, _VersionRef, _Opts) ->
            throw(#domain_conf_v2_ObjectNotFound{})
        end
    ),

    Result = dmt_client_cache:get_object(?TEST_REF, {version, 1}, #{}),
    ?assertEqual({error, object_not_found}, Result).

test_concurrent_access() ->
    meck:new(dmt_client_backend, [passthrough]),
    meck:expect(
        dmt_client_backend,
        checkout_object,
        fun(_ObjRef, _VersionRef, _Opts) ->
            % Simulate slow backend
            timer:sleep(100),
            ?TEST_VERSIONED_OBJ(1)
        end
    ),

    Version = {version, 1},
    % Start multiple concurrent requests
    Self = self(),
    Pids = [
        spawn_link(fun() ->
            Result = dmt_client_cache:get_object(?TEST_REF, Version, #{}),
            Self ! {self(), Result}
        end)
     || _ <- lists:seq(1, 5)
    ],

    % Collect results
    Results = [
        receive
            {Pid, Result} -> Result
        end
     || Pid <- Pids
    ],

    % All results should be successful and identical
    [FirstResult | RestResults] = Results,
    ?assertEqual({ok, ?TEST_VERSIONED_OBJ(1)}, FirstResult),
    ?assertEqual(lists:duplicate(length(RestResults), FirstResult), RestResults),

    % Backend should be called only once
    ?assertEqual(1, meck:num_calls(dmt_client_backend, checkout_object, '_')).

-endif.
