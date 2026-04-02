#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec plan-train-round-wind-model "$@"
fi

if [ -n "${PDGA_TRAIN_ROUND_WIND_MODEL_ARGS:-}" ]; then
    exec sh -c "plan-train-round-wind-model $PDGA_TRAIN_ROUND_WIND_MODEL_ARGS"
fi

exec plan-train-round-wind-model --help
