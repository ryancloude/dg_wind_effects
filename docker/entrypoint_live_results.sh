#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec python -m ingest_pdga_live_results.runner "$@"
fi

if [ -n "${PDGA_LIVE_RESULTS_ARGS:-}" ]; then
    # Intentionally rely on shell word splitting for normal CLI flags.
    exec sh -c "python -m ingest_pdga_live_results.runner $PDGA_LIVE_RESULTS_ARGS"
fi

exec python -m ingest_pdga_live_results.runner --help