#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec plan-silver-live-results "$@"
fi

if [ -n "${PDGA_SILVER_LIVE_RESULTS_ARGS:-}" ]; then
    exec sh -c "plan-silver-live-results $PDGA_SILVER_LIVE_RESULTS_ARGS"
fi

exec plan-silver-live-results --help