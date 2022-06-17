-module(dmt_client_fixtures).

-export([domain_with_all_fixtures/0]).
-export([domain_insert/1]).
-export([domain_insert/2]).
-export([fixture/1]).
-export([fixture_domain_object/2]).
-export([fixture_object_ref/1]).
-export([fixture_category_object/2]).
-export([fixture_category_ref/1]).
-export([fixture_currency_object/2]).
-export([fixture_currency_ref/1]).

-include_lib("damsel/include/dmsl_domain_thrift.hrl").

-spec domain_with_all_fixtures() -> dmt_client:domain().
domain_with_all_fixtures() ->
    domain_insert(
        [
            fixture(category),
            fixture(category2),
            fixture(currency)
        ]
    ).

-spec domain_insert([dmt_client:domain_object()]) -> dmt_client:domain().
domain_insert(Objects) ->
    domain_insert(Objects, dmt_domain:new()).

-spec domain_insert([dmt_client:domain_object()], dmt_client:domain()) -> dmt_client:domain().
domain_insert(Object, Domain) when not is_list(Object) ->
    domain_insert([Object], Domain);
domain_insert(Objects, Domain) ->
    lists:foldl(
        fun(Object, DomainIn) ->
            {ok, DomainOut} = dmt_domain:insert(Object, DomainIn),
            DomainOut
        end,
        Domain,
        Objects
    ).

-spec fixture(atom()) -> dmt_client:domain_object().
fixture(ID) ->
    maps:get(
        ID,
        #{
            category => fixture_category_object(1, <<"cat">>),
            category2 => fixture_category_object(2, <<"dog">>),
            currency => fixture_currency_object(<<"USD">>, #{
                name => <<"dog">>,
                numeric_code => 840,
                exponent => 2
            })
        }
    ).

-spec fixture_domain_object(binary(), binary()) -> dmt_client:domain_object().
fixture_domain_object(Ref, Data) ->
    fixture_category_object(Ref, Data).

-spec fixture_object_ref(binary()) -> {category, dmt_client:object_ref()}.
fixture_object_ref(Ref) ->
    fixture_category_ref(Ref).

-spec fixture_category_object(binary(), binary()) -> {category, dmsl_domain_thrift:'CategoryObject'()}.
fixture_category_object(Ref, Data) ->
    {category, #'domain_CategoryObject'{
        ref = #'domain_CategoryRef'{id = Ref},
        data = #'domain_Category'{name = Data, description = Data}
    }}.

-spec fixture_category_ref(binary()) -> {category, dmsl_domain_thrift:'CategoryRef'()}.
fixture_category_ref(Ref) ->
    {category, #'domain_CategoryRef'{id = Ref}}.

-spec fixture_currency_object(binary(), map()) -> {currency, dmsl_domain_thrift:'CurrencyObject'()}.
fixture_currency_object(Ref, Data) ->
    {currency, #'domain_CurrencyObject'{
        ref = #'domain_CurrencyRef'{symbolic_code = Ref},
        data = #'domain_Currency'{
            name = maps:get(name, Data, <<"some currency">>),
            symbolic_code = Ref,
            numeric_code = maps:get(numeric_code, Data, 0),
            exponent = maps:get(exponent, Data, 42)
        }
    }}.

-spec fixture_currency_ref(binary()) -> {currency, dmsl_domain_thrift:'CurrencyRef'()}.
fixture_currency_ref(Ref) ->
    {currency, #'domain_CurrencyRef'{symbolic_code = Ref}}.
