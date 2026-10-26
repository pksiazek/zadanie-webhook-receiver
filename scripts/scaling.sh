#!/usr/bin/env bash
# Horizontal scaling test (spec §13): for N = 1, 2, 3 workers, start a
# clean stack, flood the queue and measure how fast it drains.
# Runs on the host: the loadtest container has no Docker access.
#
# Usage: bash scripts/scaling.sh [N ...]      (default: 1 2 3)
# Extra flood flags can be passed via FLOOD_ARGS, e.g.
#   FLOOD_ARGS="--measure 60" bash scripts/scaling.sh 1
# RESULTS_DIR (default: results) redirects all output files, e.g. for a
# trial run that must not overwrite the committed results.
# A failed flood does not stop the run: its row shows FAIL in the
# report and the script exits non-zero at the end.
set -euo pipefail

cd "$(dirname "$0")/.."
RESULTS_DIR="${RESULTS_DIR:-results}"
mkdir -p "$RESULTS_DIR"

counts=("$@")
if [ ${#counts[@]} -eq 0 ]; then
    counts=(1 2 3)
fi
read -r -a flood_args <<< "${FLOOD_ARGS:-}"
# Seconds after the flood starts to sample worker CPU (mid-window).
STATS_AFTER="${STATS_AFTER:-70}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-180}"

# Background `docker stats` sampler of the current N (subshell + sleep).
stats_pid=""
stop_stats() {
    if [ -n "$stats_pid" ]; then
        pkill -P "$stats_pid" 2> /dev/null || true
        kill "$stats_pid" 2> /dev/null || true
        wait "$stats_pid" 2> /dev/null || true
        stats_pid=""
    fi
}
trap stop_stats EXIT

# Row for an N without a usable measurement, so the report shows it.
placeholder() {
    printf '{"workers": %d, "throughput_per_min": null, "valid": false,' \
        "$1" > "$2"
    printf ' "error": "%s"}\n' "$3" >> "$2"
}

failed=()
for n in "${counts[@]}"; do
    echo "== scaling: $n worker(s)" >&2
    result="$RESULTS_DIR/scaling-$n.json"
    observations="$RESULTS_DIR/scaling-$n-observations.txt"
    if ! docker compose down -v --remove-orphans >&2 \
        || ! docker compose up -d --build --wait \
            --wait-timeout "$WAIT_TIMEOUT" --scale "worker=$n" >&2; then
        echo "stack for $n worker(s) did not come up" >&2
        failed+=("$n")
        placeholder "$n" "$result" "stack up failed"
        continue
    fi

    (
        sleep "$STATS_AFTER"
        echo "# docker stats at flood+${STATS_AFTER}s"
        # shellcheck disable=SC2046 # one argument per container id
        docker stats --no-stream \
            --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' \
            $(docker compose ps -q)
    ) > "$observations" &
    stats_pid=$!

    status=0
    docker compose run --rm -T loadtest flood --workers "$n" \
        "${flood_args[@]}" > "$result" || status=$?
    if [ "$status" -ne 0 ]; then
        echo "flood with $n worker(s) failed (exit $status)" >&2
        failed+=("$n")
        # A crash may leave no JSON; keep a row so the report shows it.
        if [ ! -s "$result" ]; then
            placeholder "$n" "$result" "flood exited with status $status"
        fi
    fi
    # Stop the sampler if the flood ended before it fired.
    stop_stats

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
    reports+=("$RESULTS_DIR/scaling-$n.json")
done
# Results go in over stdin (one JSON object per line): no bind mount,
# so neither the container uid nor the daemon's view of $PWD matters.
# Each line gets a "source" field naming its file, so FAIL reasons in
# the report point at the file (paths must not contain double quotes).
labelled() {
    local file line
    for file in "$@"; do
        line=$(< "$file") || continue
        if [[ $line == "{"* ]]; then
            printf '{"source": "%s", %s\n' "$file" "${line#\{}"
        else
            printf '%s\n' "$line"
        fi
    done
}
report_status=0
# --no-deps: the report needs no api, even if the last stack failed.
labelled "${reports[@]}" \
    | docker compose run --rm -T --no-deps loadtest scaling-report - \
    | tee "$RESULTS_DIR/scaling-report.txt" || report_status=$?

if [ ${#failed[@]} -gt 0 ]; then
    echo "scaling: failed for N = ${failed[*]}" >&2
    exit 1
fi
exit "$report_status"
