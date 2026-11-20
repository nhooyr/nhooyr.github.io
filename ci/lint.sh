#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

if [ "${GITHUB_ACTIONS-}" ]; then
	sudo apt-get install --yes shfmt
fi

shfmt -f=0 . | xargs -0 -n 1 sh -n

go vet ./...
