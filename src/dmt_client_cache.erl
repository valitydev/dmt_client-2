-module(dmt_client_cache).

-behaviour(gen_server).

%% API

-export([start_link/0]).

-export([get_object/3]).

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
    {keypos, {#object.vsn, #object.ref}}
]).

-type timestamp() :: integer().

-record(object, {
    vsn :: dmt_client:vsn(),
    ref :: dmt_client:object_ref() | ets_match(),
    obj :: dmt_client:domain_object() | ets_match(),
    created_at :: dmt_client:vsn_created_at(),
    last_access :: timestamp()
}).

-type object() :: #object{}.

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

-spec get_object(dmt_client:vsn(), dmt_client:object_ref(), dmt_client:opts()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found | woody_error()}.
get_object(Version, ObjectRef, Opts) ->
    case ensure_object_version(Version, ObjectRef, Opts) of
        {ok, {Version, ObjectRef}} -> do_get_object(ObjectRef, Version);
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
    case ets:member(?TABLE, {Version, ObjectRef}) of
        true ->
            {reply, {ok, {ObjectRef, Version}}, State};
        false ->
            {noreply, fetch_by_reference(Version, ObjectRef, From, Opts, State)}
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

ensure_object_version(ObjectRef, Version, Opts) ->
    case ets:member(?TABLE, {ObjectRef, Version}) of
        true -> {ok, {ObjectRef, Version}};
        false -> call({fetch_object_version, ObjectRef, Version, Opts})
    end.

-spec do_get_object(dmt_client:vsn(), dmt_client:object_ref()) ->
    {ok, dmt_client:domain_object()} | {error, version_not_found | object_not_found}.
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
    Prev =
        case maps:find({ObjectRef, VersionReference}, Waiters) of
            error ->
                _Pid = schedule_fetch(ObjectRef, VersionReference, Opts),
                [];
            {ok, List} ->
                List
        end,
    Waiters#{{ObjectRef, VersionReference} => [{ReplyTo, DispatchFun} | Prev]}.

schedule_fetch(ObjectRef, VersionReference, Opts) ->
    spawn_link(
        fun() ->
            Result =
                case fetch(VersionReference, ObjectRef, Opts) of
                    {ok, #domain_conf_v2_VersionedObject{
                        global_version = Version,
                        object = Object,
                        created_at = CreatedAt
                    }} ->
                        put_object_into_table(ObjectRef, Version, Object, CreatedAt),
                        %% This will be called every time some new object is required.
                        %% Maybe consider alternative
                        cast(cleanup),
                        {ok, ObjectRef, Version};
                    {error, _} = Error ->
                        Error
                end,

            cast({dispatch, {ObjectRef, VersionReference}, Result})
        end
    ).

fetch(VersionReference, ObjectRef, Opts) ->
    try
        dmt_client_backend:checkout_object(VersionReference, ObjectRef, Opts)
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
    MaxSnapshots = maps:get(max_objects, Config),
    MaxMemory = maps:get(max_memory, Config),
    case ObjectCount > MaxSnapshots orelse (ObjectCount > 1 andalso MemoryUsage > MaxMemory) of
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
%%
%%-ifdef(TEST).
%%
%%-include_lib("eunit/include/eunit.hrl").
%%-include_lib("damsel/include/dmsl_domain_thrift.hrl").
%%
%%-type testcase() :: function() | {_Loc, function()} | [testcase()] | {setup, function(), testcase()}.
%%
%%% dirty hack for warn_missing_spec
%%-spec test() -> any().
%%
%%-spec all_test_() -> testcase().
%%
%%all_test_() ->
%%    {setup,
%%        fun() ->
%%            create_tables(),
%%            %% So that put_snapshot works correctly
%%            register(?SERVER, self())
%%        end,
%%        [
%%            fun test_cleanup/0,
%%            fun test_last_access/0,
%%            fun test_get_object/0,
%%            fun test_get_object_by_type/0,
%%            fun test_fold/0
%%        ]}.
%%
%%set_cache_limits(Elements) ->
%%    set_cache_limits(Elements, 52428800).
%%
%%set_cache_limits(Elements, Memory) ->
%%    application:set_env(dmt_client, max_cache_size, #{elements => Elements, memory => Memory}).
%%
%%cleanup() ->
%%    cleanup(#state{config = build_config()}).
%%
%%-spec test_cleanup() -> _.
%%test_cleanup() ->
%%    set_cache_limits(2),
%%    CreatedAt = <<"2024-05-14T10:00:00+03:00">>,
%%    ok = put_snapshot(#domain_conf_Snapshot{version = 4, domain = dmt_domain:new(), created_at = CreatedAt}),
%%    ok = put_snapshot(#domain_conf_Snapshot{version = 3, domain = dmt_domain:new()}),
%%    ok = put_snapshot(#domain_conf_Snapshot{version = 2, domain = dmt_domain:new()}),
%%    ok = put_snapshot(#domain_conf_Snapshot{version = 1, domain = dmt_domain:new()}),
%%    cleanup(),
%%    [
%%        #snap{vsn = 1, _ = _},
%%        #snap{vsn = 4, created_at = CreatedAt, _ = _}
%%    ] = get_all_objects().
%%
%%-spec test_last_access() -> _.
%%test_last_access() ->
%%    set_cache_limits(3),
%%    CreatedAt = <<"2024-05-14T10:00:00+03:00">>,
%%    % Tables already created in test_cleanup/0
%%    ok = put_snapshot(#domain_conf_Snapshot{version = 4, domain = dmt_domain:new(), created_at = CreatedAt}),
%%    ok = put_snapshot(#domain_conf_Snapshot{version = 3, domain = dmt_domain:new()}),
%%    ok = put_snapshot(#domain_conf_Snapshot{version = 2, domain = dmt_domain:new()}),
%%    Ref = {category, #domain_CategoryRef{id = 1}},
%%    {error, object_not_found} = get_object(3, Ref, #{}),
%%    ok = put_snapshot(#domain_conf_Snapshot{version = 1, domain = dmt_domain:new()}),
%%    cleanup(),
%%    [
%%        #snap{vsn = 1, _ = _},
%%        #snap{vsn = 3, _ = _},
%%        #snap{vsn = 4, created_at = CreatedAt, _ = _}
%%    ] = get_all_objects().
%%
%%-spec test_get_object() -> _.
%%test_get_object() ->
%%    set_cache_limits(1),
%%    Version = 5,
%%    Cat = {_, {_, Ref, _}} = dmt_client_fixtures:fixture(category),
%%    Domain = dmt_client_fixtures:domain_insert(Cat),
%%
%%    ok = put_snapshot(#domain_conf_Snapshot{version = Version, domain = Domain}),
%%    {ok, Cat} = get_object(Version, {category, Ref}, #{}).
%%
%%-spec test_get_object_by_type() -> _.
%%test_get_object_by_type() ->
%%    set_cache_limits(1),
%%    Version = 6,
%%    {_, Cat1} = dmt_client_fixtures:fixture(category),
%%    {_, Cat2} = dmt_client_fixtures:fixture(category2),
%%
%%    Domain = dmt_client_fixtures:domain_with_all_fixtures(),
%%
%%    ok = put_snapshot(#domain_conf_Snapshot{version = Version, domain = Domain}),
%%
%%    {ok, Objects} = get_objects_by_type(Version, category, #{}),
%%    [Cat1, Cat2] = lists:sort(Objects).
%%
%%-spec test_fold() -> _.
%%test_fold() ->
%%    set_cache_limits(1),
%%    Version = 7,
%%
%%    Domain = dmt_client_fixtures:domain_with_all_fixtures(),
%%
%%    ok = put_snapshot(#domain_conf_Snapshot{version = Version, domain = Domain}),
%%
%%    {ok, OrdSet} = fold_objects(
%%        Version,
%%        fun
%%            (
%%                category,
%%                #'domain_CategoryObject'{
%%                    ref = #'domain_CategoryRef'{id = ID}
%%                },
%%                Acc
%%            ) ->
%%                ordsets:add_element(ID, Acc);
%%            (_Type, _Obj, Acc) ->
%%                Acc
%%        end,
%%        ordsets:new(),
%%        #{}
%%    ),
%%
%%    [1, 2] = ordsets:to_list(OrdSet).
%%
%%% TEST
%%-endif.
