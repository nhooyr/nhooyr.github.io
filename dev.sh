#!/bin/sh
set -eu
cd "$(dirname "$0")"

{
	for i in $(seq 1 20); do
		if curl -fsI "http://$(hostname)" >/dev/null; then
			open "http://$(hostname)"
			break
		fi
		sleep 0.1
	done
} &
caddy run --watch .
