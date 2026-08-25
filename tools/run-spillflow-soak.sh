#!/usr/bin/env bash
set -euo pipefail

DURATION_SECONDS="${DURATION_SECONDS:-21600}"
INPUT_RECORDS="${INPUT_RECORDS:-10000000}"
SPILL_MEMORY_MB="${SPILL_MEMORY_MB:-64}"
OUTPUT_ROOT="${OUTPUT_ROOT:-artifacts/soak-linux}"
MAX_OPERATION_SECONDS="${MAX_OPERATION_SECONDS:-900}"
PROJECT="benchmarks/SpillFlow.ResourceBenchmarks/SpillFlow.ResourceBenchmarks.csproj"
RUN_DIR="$OUTPUT_ROOT/$(date -u +%Y%m%d-%H%M%S)"
RAW="$RUN_DIR/measurements.ndjson"
STATUS="$RUN_DIR/status.json"
mkdir -p "$RUN_DIR"

dotnet build "$PROJECT" -c Release --no-restore -p:NoWarn=NU1900
export DOTNET_GCHeapHardLimit=0x40000000
started_epoch="$(date +%s)"
deadline=$((started_epoch + DURATION_SECONDS))
completed=0
declare -A signatures
operations=(distinct orderby exceptby groupjoin aggregate)

fail() {
  printf '{"state":"failed","completedRuns":%d,"message":"%s"}\n' "$completed" "$1" > "$STATUS"
  echo "FAIL: $1" >&2
  exit 1
}

while (( $(date +%s) < deadline )); do
  for operation in "${operations[@]}"; do
    (( $(date +%s) >= deadline )) && break
    run_started="$(date +%s)"
    measurement="$(dotnet run -c Release --no-build --project "$PROJECT" -- \
      spill "$operation" "$INPUT_RECORDS" "$SPILL_MEMORY_MB" | tail -n 1)"
    elapsed=$(( $(date +%s) - run_started ))
    (( elapsed <= MAX_OPERATION_SECONDS )) || fail "$operation exceeded $MAX_OPERATION_SECONDS seconds; host may have paused"
    output_records="$(printf '%s' "$measurement" | sed -n 's/.*"OutputRecords":\([0-9-]*\).*/\1/p')"
    checksum="$(printf '%s' "$measurement" | sed -n 's/.*"Checksum":\([0-9-]*\).*/\1/p')"
    [[ -n "$output_records" && -n "$checksum" ]] || fail "$operation returned invalid JSON"
    signature="$output_records:$checksum"
    if [[ -n "${signatures[$operation]:-}" && "${signatures[$operation]}" != "$signature" ]]; then
      fail "$operation output count or checksum drifted"
    fi
    signatures[$operation]="$signature"
    printf '%s\n' "$measurement" >> "$RAW"
    completed=$((completed + 1))
    printf '{"state":"running","completedRuns":%d,"updatedUtc":"%s"}\n' \
      "$completed" "$(date -u +%FT%TZ)" > "$STATUS"
    echo "$(date -u +%FT%TZ) $operation pass ($signature)"
  done
done

printf '{"state":"completed","completedRuns":%d,"startedUtc":"%s","completedUtc":"%s"}\n' \
  "$completed" "$(date -u -d "@$started_epoch" +%FT%TZ)" "$(date -u +%FT%TZ)" > "$STATUS"
echo "Linux soak completed: $STATUS"
