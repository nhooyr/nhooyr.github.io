#!/bin/sh
set -eu
cd "$(dirname "$0")"

if [ "${GITHUB_ACTIONS-}" ]; then
	sudo apt-get install --yes shfmt
fi

npx prettier -w --print-width=120 .
shfmt -s -w .

if [ "${CI-}" ]; then
	git add --intent-to-add --ignore-removal .

	tput() { command tput -Txterm-256color "$@"; }

	if ! git --no-pager diff --color=always --stat --patch --exit-code; then
		tput bold
		printf '%s %s: unexpected diff\n' "$(printf '%40s' | tr ' ' '^')" "${0##*/}"
		tput sgr0
		exit 1
	fi
fi
