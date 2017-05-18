-module(dmt_client_cache).
-behaviour(gen_server).

%%

-export([start_link/0]).

-export([put/1]).
-export([get/1]).
-export([get_latest/0]).
-export([update/0]).

%%

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TABLE, ?MODULE).
-define(SERVER, ?MODULE).
-define(DEFAULT_INTERVAL, 5000).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%

-spec start_link() -> {ok, pid()} | {error, term()}. % FIXME

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec put(dmt_client:snapshot()) -> dmt_client:snapshot().

put(Snapshot) ->
    ok = gen_server:cast(?SERVER, {put, Snapshot}),
    Snapshot.

-spec get(dmt_client:version()) -> {ok, dmt_client:snapshot()} | {error, version_not_found}.

get(Version) ->
    get_snapshot(Version).

-spec get_latest() -> {ok, dmt_client:snapshot()} | {error, version_not_found}.

get_latest() ->
    case latest_snapshot() of
        {ok, Snapshot} ->
            {ok, Snapshot};
        {error, version_not_found} ->
            gen_server:call(?SERVER, get_latest)
    end.

-spec update() -> {ok, dmt_client:version()} | {error, term()}.

update() ->
    gen_server:call(?SERVER, update).

%%

-record(state, {
    timer = undefined :: undefined | reference()
}).

-type state() :: #state{}.

-spec init(_) -> {ok, state(), 0}.

init(_) ->
    EtsOpts = [
        named_table,
        ordered_set,
        protected,
        {read_concurrency, true},
        {keypos, #'Snapshot'.version}
    ],
    ?TABLE = ets:new(?TABLE, EtsOpts),
    {ok, #state{}, 0}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.

handle_call(update, _From, State) ->
    {reply, update_cache(), restart_timer(State)};

handle_call(get_latest, _From, State) ->
    {reply, latest_snapshot(), State};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast({put, Snapshot}, State) ->
    ok = put_snapshot(Snapshot),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.

handle_info(timeout, State) ->
    _Result = update_cache(),
    {noreply, restart_timer(State)};

handle_info(_Msg, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {error, noimpl}.
code_change(_OldVsn, _State, _Extra) ->
    {error, noimpl}.

%% internal

-spec put_snapshot(dmt_client:snapshot()) -> ok.

put_snapshot(Snapshot) ->
    true = ets:insert(?TABLE, Snapshot),
    cleanup().

-spec get_snapshot(dmt_client:version()) -> {ok, dmt_client:snapshot()} | {error, version_not_found}.

get_snapshot(Version) ->
    case ets:lookup(?TABLE, Version) of
        [Snapshot] ->
            {ok, Snapshot};
        [] ->
            {error, version_not_found}
    end.

-spec latest_snapshot() -> {ok, dmt_client:snapshot()} | {error, version_not_found}.

latest_snapshot() ->
    case ets:last(?TABLE) of
        '$end_of_table' ->
            {error, version_not_found};
        Version ->
            get_snapshot(Version)
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

-spec update_cache() -> {ok, dmt_client:version()} | {error, term()}.

update_cache() ->
    try
        NewHead = case latest_snapshot() of
            {ok, OldHead} ->
                FreshHistory = dmt_client_api:pull(OldHead#'Snapshot'.version),
                dmt_history:head(FreshHistory, OldHead);
            {error, version_not_found} ->
                dmt_client_api:checkout({head, #'Head'{}})
        end,
        ok = put_snapshot(NewHead),
        {ok, NewHead#'Snapshot'.version}
    catch
        error:{woody_error, {_Source, Class, _Details}} = Error when
            Class == resource_unavailable;
            Class == result_unknown
        ->
            {error, Error}
    end.

-spec cleanup() -> ok.

cleanup() ->
    {Elements, Memory} = get_cache_size(),
    CacheLimits = genlib_app:env(dmt_client, max_cache_size),
    MaxElements = genlib_map:get(elements, CacheLimits, 20),
    MaxMemory = genlib_map:get(memory, CacheLimits, 52428800), % 50Mb by default
    case Elements > MaxElements orelse Memory > MaxMemory of
        true ->
            ok = remove_earliest(),
            cleanup();
        false ->
            ok
    end.

-spec get_cache_size() -> {non_neg_integer(), non_neg_integer()}.

get_cache_size() ->
    WordSize = erlang:system_info(wordsize),
    Info = ets:info(?TABLE),
    {proplists:get_value(size, Info), WordSize * proplists:get_value(memory, Info)}.

-spec remove_earliest() -> ok.

remove_earliest() ->
    % Naive implementation, but probably good enough
    remove_earliest(ets:first(?TABLE)).

-spec remove_earliest('$end_of_table' | dmt_client:version()) -> ok.

remove_earliest('$end_of_table') ->
    ok;
remove_earliest(Key) ->
    true = ets:delete(?TABLE, Key),
    ok.
