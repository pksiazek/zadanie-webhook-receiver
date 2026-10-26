#!/usr/bin/env bash
# Horizontal scaling test (spec §13): for N = 1, 2, 3 workers, start a
# clean stack, flood the queue and measure how fast it drains.
# Runs on the host: the loadtest container has no Docker access.
#
# Usage: bash scripts/scaling.sh [N ...]      (default: 1 2 3)
# Extra flood flags can be passed via FLOOD_ARGS, e.g.
#   FLOOD_ARGS="--measure 60" bash scripts/scaling.sh
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p results

counts=("$@")
if [ ${#counts[@]} -eq 0 ]; then
    counts=(1 2 3)
fi
read -r -a flood_args <<< "${FLOOD_ARGS:-}"
# Seconds after the flood starts to sample worker CPU (mid-window).
STATS_AFTER="${STATS_AFTER:-70}"

for n in "${counts[@]}"; do
    echo "== scaling: $n worker(s)" >&2
    docker compose down -v --remove-orphans >&2
    docker compose up -d --build --wait --scale "worker=$n" >&2

    observations="results/scaling-$n-observations.txt"
    (
        sleep "$STATS_AFTER"
        echo "# docker stats at flood+${STATS_AFTER}s"
        # shellcheck disable=SC2046 # one argument per container id
        docker stats --no-stream \
            --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' \
            $(docker compose ps -q)
    ) > "$observations" &
    stats_pid=$!

    docker compose run --rm -T loadtest flood --workers "$n" \
        "${flood_args[@]}" > "results/scaling-$n.json"
    wait "$stats_pid"

    # Contention evidence (log lines carry ids only, never payloads).
    {
        echo "# worker log counts"
        for pattern in "database is locked" "worker turn failed" \
            "recovered expired leases"; do
            count=$(docker compose logs --no-color worker \
                | grep -c "$pattern" || true)
            echo "$pattern: $count"
        done
    } >> "$observations"
done

reports=()
for n in "${counts[@]}"; do
    reports+=("/results/scaling-$n.json")
done
docker compose run --rm -T -v "$PWD/results:/results:ro" loadtest \
    scaling-report "${reports[@]}" \
    | tee results/scaling-report.txt
