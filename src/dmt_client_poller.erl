-module(dmt_client_poller).
-behaviour(gen_server).

-export([start_link/0]).
-export([poll/0]).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(SERVER, ?MODULE).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec poll() -> {ok, dmt:version()} | {error, term()}.
poll() ->
    gen_server:call(?SERVER, poll).

-record(state, {
    timer :: reference()
}).

-type state() :: #state{}.

-spec init(_) -> {ok, state()}.

init(_) ->
    {ok, start_timer(#state{})}.

-spec handle_call(poll, {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call(poll, _From, State) ->
    {reply, pull_safe(), restart_timer(State)}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(poll, state()) -> {noreply, state()}.
handle_info(poll, State) ->
    _Result = pull_safe(),
    {noreply, restart_timer(State)}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {error, noimpl}.
code_change(_OldVsn, _State, _Extra) ->
    {error, noimpl}.

%% Internal

-define(DEFAULT_INTERVAL, 5000).

-spec restart_timer(#state{}) -> #state{}.
restart_timer(State = #state{timer = undefined}) ->
    start_timer(State);
restart_timer(State = #state{timer = TimerRef}) ->
    _ = erlang:cancel_timer(TimerRef),
    start_timer(State#state{timer = undefined}).

-spec start_timer(#state{}) -> #state{}.
start_timer(State = #state{timer = undefined}) ->
    Interval = genlib_app:env(dmt_client, poll_interval, ?DEFAULT_INTERVAL),
    State#state{timer = erlang:send_after(Interval, self(), poll)}.

-spec pull() -> dmt:version().
pull() ->
    OldHead = try 
        dmt_cache:checkout_head()
    catch
        version_not_found ->
            #'Snapshot'{version = 0, domain = dmt_domain:new()}
    end,
    FreshHistory = dmt_client_api:pull(OldHead#'Snapshot'.version),
    #'Snapshot'{version = NewLastVersion} = NewHead = dmt_history:head(FreshHistory, OldHead),
    _ = dmt_cache:cache_snapshot(NewHead),
    NewLastVersion.

-spec pull_safe() -> {ok, dmt:version()} | {error, term()}.

pull_safe() ->
    try
        NewLastVersion = pull(),
        {ok, NewLastVersion}
    catch
        error:{transport_error, Reason} when
            % denial of service
            Reason == server_error;
            Reason == service_unavailable;
            Reason == econnrefused;
            % undefined result
            Reason == timeout;
            Reason == closed; % only if received after successful tcp send, otherwise denial of service
            Reason == partial_response
        ->
            {error, Reason};
        % undefined result
        error:{transport_error, {http_code, 504}} ->
            {error, gateway_timeout}
    end.
