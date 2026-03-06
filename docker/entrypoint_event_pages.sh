#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec ingest-pdga-event-pages "$@"
fi

if [ -n "${PDGA_INGEST_ARGS:-}" ]; then
    # Intentionally rely on shell word splitting so users can pass normal CLI flags.
    # Example: PDGA_INGEST_ARGS="--range 1000-1010 --dry-run"
    exec sh -c "ingest-pdga-event-pages $PDGA_INGEST_ARGS"
fi

exec ingest-pdga-event-pages --help