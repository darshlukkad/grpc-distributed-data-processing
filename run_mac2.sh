#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Installing dependencies ==="
if ! command -v brew &>/dev/null; then
    echo "Homebrew not found. Install from https://brew.sh then re-run."
    exit 1
fi

brew install grpc nlohmann-json libomp 2>/dev/null || true

echo "=== Building C++ nodes ==="
rm -rf cpp/build
mkdir -p cpp/build
cd cpp/build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(sysctl -n hw.logicalcpu)
cd "$SCRIPT_DIR"

echo "=== Generating Python stubs ==="
pip3 install grpcio-tools --quiet
cd proto
python3 -m grpc_tools.protoc -I. \
    --python_out=../python \
    --grpc_python_out=../python \
    mini2.proto
cd "$SCRIPT_DIR"

echo "=== Installing Python dependencies ==="
pip3 install -r python/requirements.txt --quiet

echo "=== Starting nodes F, G, H (C++) ==="
for id in F G H; do
    cpp/build/node_server --id $id --config config/topology.json \
        > /tmp/node_${id}.log 2>&1 &
    echo "Node $id started (pid $!)"
done

echo "=== Waiting for F, G, H to load data (may take ~60s) ==="
for id in F G H; do
    while ! grep -q "Listening on port" /tmp/node_${id}.log 2>/dev/null; do
        sleep 2
    done
    echo "Node $id ready: $(tail -1 /tmp/node_${id}.log)"
done

echo "=== Starting node I (Python) ==="
python3 python/node_server.py --id I --config config/topology.json