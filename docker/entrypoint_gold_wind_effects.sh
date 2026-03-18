#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec plan-gold-wind-effects "$@"
fi

if [ -n "${PDGA_GOLD_WIND_EFFECTS_ARGS:-}" ]; then
    exec sh -c "plan-gold-wind-effects $PDGA_GOLD_WIND_EFFECTS_ARGS"
fi

exec plan-gold-wind-effects --help