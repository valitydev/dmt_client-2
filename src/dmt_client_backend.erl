-module(dmt_client_backend).

-export([commit/4]).
-export([checkout_object/3]).

%%% Behaviour callbacks

-callback commit(
    dmt_client:vsn(),
    dmt_client:commit(),
    dmt_client:user_op_id(),
    dmt_client:opts()
) -> dmt_client:commit_response() | no_return().

-callback checkout_object(dmt_client:object_ref(), dmt_client:vsn(), dmt_client:opts()) ->
    dmt_client:versioned_object() | no_return().

%%% API

-spec commit(
    dmt_client:base_version(),
    dmt_client:commit(),
    dmt_client:user_op_id(),
    dmt_client:opts()
) -> dmt_client:commit_response() | no_return().
commit(Version, Commit, UserOpID, Opts) ->
    call(commit, [Version, Commit, UserOpID, Opts]).

-spec checkout_object(dmt_client:object_ref(), dmt_client:vsn(), dmt_client:opts()) ->
    dmt_client:versioned_object() | no_return().
checkout_object(ObjectReference, VersionReference, Opts) ->
    call(checkout_object, [ObjectReference, VersionReference, Opts]).

%%% Internal functions

-spec get_api_module() -> module().
get_api_module() ->
    genlib_app:env(dmt_client, api_module, dmt_client_api).

-spec call(atom(), list()) -> term() | no_return().
call(Fun, Args) ->
    Module = get_api_module(),
    erlang:apply(Module, Fun, Args).
