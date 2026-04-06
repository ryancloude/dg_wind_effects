#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec plan-score-round-wind-model "$@"
fi

if [ -n "${PDGA_SCORE_ROUND_WIND_MODEL_ARGS:-}" ]; then
    exec sh -c "plan-score-round-wind-model $PDGA_SCORE_ROUND_WIND_MODEL_ARGS"
fi

exec plan-score-round-wind-model --help