PACKAGE ?= ext
VERSION ?= $(shell git describe --tags)
BASE_DIR ?= $(shell pwd)
ERLANG_BIN ?= $(shell dirname $(shell which erl))
REBAR ?= $(BASE_DIR)/rebar3
MAKE = make

.PHONY: compile clean packageclean check lint shell

all: compile

compile:
	$(REBAR) compile

debug_bin:
	$(REBAR) as debug_bin compile

clean: packageclean
	$(REBAR) clean

generate:
	$(REBAR) as dev compile
	_build/dev/lib/gpb/bin/protoc-erl -strbin -no-gen-introspect -pkgs -modsuffix _proto -maps -I. proto/*.proto -o src/

packageclean:
	rm -fr *.deb
	rm -fr *.tar.gz

check: xref dialyzer

shell:
	$(REBAR) shell --apps $(PACKAGE)

include tools.mk
