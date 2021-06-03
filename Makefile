REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = builtils
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

UTILS_PATH := builtils
TEMPLATES_PATH := .

# Name of the service
SERVICE_NAME := dmt_client

# Build image tag to be used
BUILD_IMAGE_NAME := build-erlang
BUILD_IMAGE_TAG := eb6f9920868599f7e1a8ee9aaedb1921a027f7a0

CALL_ANYWHERE := all submodules rebar-update compile xref lint dialyze check clean distclean check_format format
CALL_W_CONTAINER := $(CALL_ANYWHERE) test

all: compile

-include $(UTILS_PATH)/make_lib/utils_container.mk

.PHONY: $(CALL_W_CONTAINER)

$(SUBTARGETS): %/.git: %
	git submodule update --init $<
	touch $@

submodules: $(SUBTARGETS)

rebar-update:
	$(REBAR) update

compile: submodules rebar-update
	$(REBAR) compile

xref: submodules
	$(REBAR) xref

lint:
	elvis rock

check_format:
	$(REBAR) fmt -c

format:
	$(REBAR) fmt -w

dialyze: submodules
	$(REBAR) as dialyze dialyzer

test: submodules
	$(REBAR) eunit
	$(REBAR) ct

check: lint xref dialyze

clean:
	$(REBAR) clean

distclean:
	rm -rfv _build
