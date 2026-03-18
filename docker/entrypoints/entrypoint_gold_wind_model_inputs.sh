#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec plan-gold-wind-model-inputs "$@"
fi

if [ -n "${PDGA_GOLD_WIND_MODEL_INPUTS_ARGS:-}" ]; then
    exec sh -c "plan-gold-wind-model-inputs $PDGA_GOLD_WIND_MODEL_INPUTS_ARGS"
fi

exec plan-gold-wind-model-inputs --help