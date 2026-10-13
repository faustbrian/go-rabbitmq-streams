SHELL := /usr/bin/env bash

GOLIB ?= golib

.PHONY: source-docs-check check check-full ci cohesion inventory repository-check specification-check specification-check-online

check: source-docs-check
	$(GOLIB) check --local --all

check-full: source-docs-check specification-check-online
	$(GOLIB) check --all

ci: source-docs-check
	$(GOLIB) repository check
	$(GOLIB) cohesion check
	$(GOLIB) specification check
	$(GOLIB) check --local --all

cohesion:
	$(GOLIB) cohesion check

inventory repository-check:
	$(GOLIB) repository check

specification-check:
	$(GOLIB) specification check

specification-check-online:
	$(GOLIB) specification check --online

source-docs-check:
	$(GOLIB) source-docs check
