-module(dmt_client_woody_event_handler).

-behaviour(woody_event_handler).

-export([handle_event/4]).

-spec handle_event(EventType, RpcID, EventMeta, Opts)
    -> _ when
        EventType :: woody_event_handler:event(),
        RpcID ::  woody:rpc_id(),
        EventMeta :: woody_event_handler:event_meta(),
        Opts :: woody:options().

handle_event(EventType, RpcID, #{status := error, class := Class, reason := Reason, stack := Stack}, Opts) ->
    lager:error(
        maps:to_list(RpcID),
        "[server] ~s with ~s:~p at ~s~nOpts: ~p",
        [EventType, Class, Reason, genlib_format:format_stacktrace(Stack, [newlines]), Opts]
    );

handle_event(EventType, RpcID, EventMeta, Opts) ->
    lager:debug(maps:to_list(RpcID), "[server] ~s: ~p~nOpts: ~p", [EventType, EventMeta, Opts]).
