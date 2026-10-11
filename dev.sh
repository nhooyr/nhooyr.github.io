#!/bin/sh
set -eu
cd "$(dirname "$0")"

caddy run --watch .