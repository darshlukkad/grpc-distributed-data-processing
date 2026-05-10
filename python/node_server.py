import argparse
import csv
import json
import os
import sys
import time
from concurrent import futures
from datetime import datetime

import grpc
import mini2_pb2
import mini2_pb2_grpc

COL_UNIQUE_KEY   = 0
COL_CREATED_DATE = 1
COL_INCIDENT_ZIP = 9
COL_LATITUDE     = 41
COL_LONGITUDE    = 42

MAX_MSG = 64 * 1024 * 1024
CHUNK   = 500


def parse_date(s):
    s = s.strip()
    if not s:
        return 0
    try:
        dt = datetime.strptime(s, '%m/%d/%Y %I:%M:%S %p')
        return int(dt.timestamp())
    except ValueError:
        return 0


def parse_zip(s):
    s = s.strip()
    if not s or not s.isdigit():
        return 0
    return int(s)


def load_data(csv_path, row_start, row_end):
    records = []
    with open(csv_path, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        next(reader)
        for i, row in enumerate(reader):
            if i < row_start:
                continue
            if i > row_end:
                break
            if len(row) <= COL_LONGITUDE:
                continue
            try:
                r = {
                    'unique_key':   int(row[COL_UNIQUE_KEY]) if row[COL_UNIQUE_KEY] else 0,
                    'created_date': parse_date(row[COL_CREATED_DATE]),
                    'incident_zip': parse_zip(row[COL_INCIDENT_ZIP]),
                    'latitude':     float(row[COL_LATITUDE])  if row[COL_LATITUDE]  else 0.0,
                    'longitude':    float(row[COL_LONGITUDE]) if row[COL_LONGITUDE] else 0.0,
                }
                records.append(r)
            except Exception:
                continue
    print(f"[Python] loaded {len(records)} records", file=sys.stderr)
    return records


def to_proto(r):
    sr = mini2_pb2.ServiceRecord()
    sr.unique_key   = r['unique_key']
    sr.created_date = r['created_date']
    sr.incident_zip = r['incident_zip']
    sr.latitude     = r['latitude']
    sr.longitude    = r['longitude']
    sr.borough      = 0
    return sr


class NodeServicer(mini2_pb2_grpc.NodeServiceServicer):

    def __init__(self, node_id, records, peers_cfg):
        self.node_id = node_id
        self.records = records
        self.peers   = peers_cfg
        self._store  = {}

    def _search_local(self, query):
        t = query.type
        out = []
        if t == mini2_pb2.ZIP_RANGE:
            out = [r for r in self.records
                   if query.zip_min <= r['incident_zip'] <= query.zip_max]
        elif t == mini2_pb2.DATE_RANGE:
            out = [r for r in self.records
                   if query.date_min <= r['created_date'] <= query.date_max]
        elif t == mini2_pb2.BBOX:
            out = [r for r in self.records
                   if query.lat_min <= r['latitude']  <= query.lat_max
                   and query.lon_min <= r['longitude'] <= query.lon_max]
        return [to_proto(r) for r in out]

    def _call_peer(self, peer, query):
        opts = [
            ('grpc.max_receive_message_length', MAX_MSG),
            ('grpc.max_send_message_length',    MAX_MSG),
        ]
        try:
            ch   = grpc.insecure_channel(peer['address'], options=opts)
            stub = mini2_pb2_grpc.NodeServiceStub(ch)
            resp = stub.Forward(query, timeout=60)
            return list(resp.records)
        except Exception as e:
            print(f"[{self.node_id}] forward to {peer['id']} failed: {e}", file=sys.stderr)
            return []

    def _gather_peers(self, query):
        if not self.peers:
            return []
        with futures.ThreadPoolExecutor(max_workers=len(self.peers)) as ex:
            futs = [ex.submit(self._call_peer, p, query) for p in self.peers]
            all_recs = []
            for f in futs:
                all_recs.extend(f.result())
        return all_recs

    def Submit(self, request, context):
        req_id = f"{self.node_id}_{time.time_ns()}"
        with futures.ThreadPoolExecutor(max_workers=2) as ex:
            local_fut = ex.submit(self._search_local, request)
            peer_recs  = self._gather_peers(request)
            local_recs = local_fut.result()
        self._store[req_id] = list(peer_recs) + local_recs
        print(f"[{self.node_id}] Submit {req_id} total={len(self._store[req_id])}",
              file=sys.stderr)
        return mini2_pb2.SubmitResponse(request_id=req_id)

    def Fetch(self, request, context):
        recs = self._store.get(request.request_id)
        if recs is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('unknown request_id')
            return mini2_pb2.FetchResponse()
        csize = request.chunk_size if request.chunk_size > 0 else CHUNK
        start = request.chunk_idx
        end   = min(len(recs), start + csize)
        resp  = mini2_pb2.FetchResponse(
            request_id=request.request_id,
            chunk_idx=request.chunk_idx,
            is_last=(end >= len(recs)),
        )
        resp.records.extend(recs[start:end])
        return resp

    def Forward(self, request, context):
        with futures.ThreadPoolExecutor(max_workers=2) as ex:
            local_fut = ex.submit(self._search_local, request)
            peer_recs  = self._gather_peers(request)
            local_recs = local_fut.result()
        resp = mini2_pb2.ForwardResponse()
        resp.records.extend(peer_recs)
        resp.records.extend(local_recs)
        print(f"[{self.node_id}] Forward returning {len(resp.records)}", file=sys.stderr)
        return resp

    def Cancel(self, request, context):
        self._store.pop(request.request_id, None)
        return mini2_pb2.CancelResponse()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id',     required=True)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    nc        = cfg['nodes'][args.id]
    port      = nc['port']
    row_start = nc['row_start']
    row_end   = nc['row_end']
    csv_path = os.path.join(os.path.dirname(os.path.abspath(args.config)), cfg['csv_path'])

    peers_cfg = []
    for pid in cfg['peers'].get(args.id, []):
        pc = cfg['nodes'][pid]
        peers_cfg.append({'id': pid, 'address': f"{pc['host']}:{pc['port']}"})

    print(f"[{args.id}] Loading rows {row_start}..{row_end}", file=sys.stderr)
    records = load_data(csv_path, row_start, row_end)

    opts = [
        ('grpc.max_receive_message_length', MAX_MSG),
        ('grpc.max_send_message_length',    MAX_MSG),
    ]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=opts)
    mini2_pb2_grpc.add_NodeServiceServicer_to_server(
        NodeServicer(args.id, records, peers_cfg), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"[{args.id}] Listening on port {port}", file=sys.stderr)
    server.wait_for_termination()


if __name__ == '__main__':
    main()