#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec ingest-weather-observations "$@"
fi

if [ -n "${PDGA_WEATHER_OBS_ARGS:-}" ]; then
    exec sh -c "ingest-weather-observations $PDGA_WEATHER_OBS_ARGS"
fi

exec ingest-weather-observations --help