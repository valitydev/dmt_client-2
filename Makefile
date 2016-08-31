REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = damsel builtils
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

UTILS_PATH := builtils
TEMPLATES_PATH := .

# Name of the service
SERVICE_NAME := dmt_client

# Build image tag to be used
BUILD_IMAGE_TAG := 753126790c9ecd763840d9fe58507335af02b875

CALL_ANYWHERE := all submodules rebar-update compile xref lint dialyze clean distclean
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

dialyze: submodules
	$(REBAR) dialyzer

test: submodules
	$(REBAR) ct

clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean -a
	rm -rfv _build _builds _cache _steps _temp
