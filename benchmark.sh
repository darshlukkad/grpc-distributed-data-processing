#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLIENT="$SCRIPT_DIR/cpp/build/node_client"
CONFIG="$SCRIPT_DIR/config/topology.json"

ZIP_CASES=(
    "10001 10499"   "10001 10499"   "10001 10499"   "10001 10499"   "10001 10499"
)
DATE_CASES=(
    "1577836800 1604188799"   "1577836800 1604188799"   "1577836800 1604188799"
    "1577836800 1604188799"   "1577836800 1604188799"
)
BBOX_CASES=(
    "40.57 40.74 -74.04 -73.83"   "40.57 40.74 -74.04 -73.83"
    "40.57 40.74 -74.04 -73.83"   "40.57 40.74 -74.04 -73.83"
    "40.57 40.74 -74.04 -73.83"
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
    echo "$avg" > /tmp/_bench_avg
}

echo ""
echo "======================================================================"
echo "  Mini2 Benchmark  —  30 queries (10 per type)"
echo "======================================================================"

run_suite "ZIP"  "${ZIP_CASES[@]}";  avg_zip=$(cat /tmp/_bench_avg)
run_suite "DATE" "${DATE_CASES[@]}"; avg_date=$(cat /tmp/_bench_avg)
run_suite "BBOX" "${BBOX_CASES[@]}"; avg_bbox=$(cat /tmp/_bench_avg)

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