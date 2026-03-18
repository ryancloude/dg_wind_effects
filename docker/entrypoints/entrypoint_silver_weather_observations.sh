#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec plan-silver-weather-observations "$@"
fi

if [ -n "${PDGA_SILVER_WEATHER_OBS_ARGS:-}" ]; then
    exec sh -c "plan-silver-weather-observations $PDGA_SILVER_WEATHER_OBS_ARGS"
fi

exec plan-silver-weather-observations --help