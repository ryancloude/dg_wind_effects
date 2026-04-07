#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec plan-report-round-weather-impacts "$@"
fi

if [ -n "${PDGA_REPORT_ROUND_WEATHER_IMPACTS_ARGS:-}" ]; then
    exec sh -c "plan-report-round-weather-impacts $PDGA_REPORT_ROUND_WEATHER_IMPACTS_ARGS"
fi

exec plan-report-round-weather-impacts --help
