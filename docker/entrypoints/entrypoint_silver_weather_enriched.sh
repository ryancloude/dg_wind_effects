#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec plan-silver-weather-enriched "$@"
fi

if [ -n "${PDGA_SILVER_WEATHER_ENRICHED_ARGS:-}" ]; then
    exec sh -c "plan-silver-weather-enriched $PDGA_SILVER_WEATHER_ENRICHED_ARGS"
fi

exec plan-silver-weather-enriched --help