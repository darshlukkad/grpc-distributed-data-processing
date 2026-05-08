#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLIENT="$SCRIPT_DIR/cpp/build/node_client"
CONFIG="$SCRIPT_DIR/config/topology.json"

ZIP_CASES=(
    "10001 10099"   "10100 10199"   "10200 10299"   "10300 10399"   "10400 10499"
    "11200 11299"   "11300 11399"   "11400 11499"   "10001 10499"   "10001 11999"
)
DATE_CASES=(
    "1577836800 1580515199"   "1580515200 1583020799"   "1583020800 1585699199"
    "1585699200 1588291199"   "1588291200 1590969599"   "1590969600 1593561599"
    "1593561600 1596239999"   "1596240000 1598918399"   "1598918400 1601510399"
    "1601510400 1604188799"
)
BBOX_CASES=(
    "40.70 40.75 -74.02 -73.97"   "40.75 40.80 -73.97 -73.92"
    "40.80 40.85 -73.95 -73.90"   "40.65 40.70 -73.98 -73.93"
    "40.60 40.65 -74.00 -73.95"   "40.55 40.60 -74.05 -74.00"
    "40.85 40.90 -73.90 -73.85"   "40.57 40.74 -74.04 -73.83"
    "40.70 40.85 -74.00 -73.85"   "40.60 40.90 -74.05 -73.80"
)

run_one() {
    "$CLIENT" --config "$CONFIG" $@ >/dev/null 2>/tmp/_bench_out
    cat /tmp/_bench_out
}

run_suite() {
    local type="$1"; shift
    local -a cases=("$@")
    local total=0 i=1

    echo ""
    echo "=== $type (10 runs) ==="
    printf "%-4s  %-36s  %12s  %8s  %8s\n" "Run" "Parameters" "Records" "Chunks" "ms"
    echo "----------------------------------------------------------------------"

    for case in "${cases[@]}"; do
        local args label
        if   [ "$type" = "ZIP"  ]; then
            read -r a b <<< "$case"
            args="--query zip --zip-min $a --zip-max $b"
            label="zip $a-$b"
        elif [ "$type" = "DATE" ]; then
            read -r a b <<< "$case"
            args="--query date --date-min $a --date-max $b"
            label="date $a..$b"
        elif [ "$type" = "BBOX" ]; then
            read -r la lb lo lp <<< "$case"
            args="--query bbox --lat-min $la --lat-max $lb --lon-min $lo --lon-max $lp"
            label="bbox $la/$lb/$lo/$lp"
        fi

        local out; out=$(run_one $args)
        local rec; rec=$(echo "$out" | grep -o 'total_records=[0-9]*' | cut -d= -f2)
        local chk; chk=$(echo "$out" | grep -o 'chunks=[0-9]*'       | cut -d= -f2)
        local ms;  ms=$(echo  "$out" | grep -o 'dt_ms=[0-9]*'        | cut -d= -f2)

        printf "%-4s  %-36s  %12s  %8s  %8s\n" "$i" "$label" "$rec" "$chk" "${ms}ms"
        total=$((total + ms))
        i=$((i + 1))
    done

    local avg=$((total / 10))
    echo "----------------------------------------------------------------------"
    printf "%-4s  %-36s  %12s  %8s  %8s\n" "AVG" "" "" "" "${avg}ms"
    echo "$avg"
}

echo ""
echo "======================================================================"
echo "  Mini2 Benchmark  —  30 queries (10 per type)"
echo "======================================================================"

avg_zip=$(run_suite  "ZIP"  "${ZIP_CASES[@]}")
avg_date=$(run_suite "DATE" "${DATE_CASES[@]}")
avg_bbox=$(run_suite "BBOX" "${BBOX_CASES[@]}")

overall=$(( (avg_zip + avg_date + avg_bbox) / 3 ))

echo ""
echo "======================================================================"
echo "  SUMMARY"
echo "======================================================================"
printf "  %-10s  %8s\n" "ZIP avg"     "${avg_zip}ms"
printf "  %-10s  %8s\n" "DATE avg"    "${avg_date}ms"
printf "  %-10s  %8s\n" "BBOX avg"    "${avg_bbox}ms"
echo "  ------------------"
printf "  %-10s  %8s\n" "OVERALL avg" "${overall}ms"
echo "======================================================================"
echo ""