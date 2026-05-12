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
COL_CLOSED_DATE  = 2
COL_AGENCY       = 3
COL_AGENCY_NAME  = 4
COL_COMPLAINT    = 5
COL_DESCRIPTOR   = 6
COL_LOC_TYPE     = 7
COL_ADDL_DETAILS = 8
COL_INCIDENT_ZIP = 9
COL_INC_ADDR     = 10
COL_STREET       = 11
COL_CROSS1       = 12
COL_CROSS2       = 13
COL_INTERSECT1   = 14
COL_INTERSECT2   = 15
COL_ADDR_TYPE    = 16
COL_CITY         = 17
COL_LANDMARK     = 18
COL_FACILITY     = 19
COL_STATUS       = 20
COL_DUE_DATE     = 21
COL_RES_DESC     = 22
COL_RES_UPDATED  = 23
COL_COMM_BOARD   = 24
COL_BBL          = 25
COL_BOROUGH      = 26
COL_X_COORD      = 27
COL_Y_COORD      = 28
COL_CHANNEL      = 29
COL_PARK_FAC     = 30
COL_PARK_BORO    = 31
COL_VEHICLE      = 32
COL_TAXI_BORO    = 33
COL_TAXI_PICKUP  = 34
COL_BRIDGE_NAME  = 35
COL_BRIDGE_DIR   = 36
COL_ROAD_RAMP    = 37
COL_BRIDGE_SEG   = 38
COL_LATITUDE     = 41
COL_LONGITUDE    = 42
COL_LOCATION     = 43

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


def safe_int(s, default=0):
    try: return int(s.strip()) if s.strip() else default
    except: return default

def safe_float(s, default=0.0):
    try: return float(s.strip()) if s.strip() else default
    except: return default

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
                def g(c): return row[c].strip() if c < len(row) else ''
                r = {
                    'unique_key':            safe_int(g(COL_UNIQUE_KEY)),
                    'created_date':          parse_date(g(COL_CREATED_DATE)),
                    'closed_date':           parse_date(g(COL_CLOSED_DATE)),
                    'due_date':              parse_date(g(COL_DUE_DATE)),
                    'resolution_updated_date': parse_date(g(COL_RES_UPDATED)),
                    'incident_zip':          parse_zip(g(COL_INCIDENT_ZIP)),
                    'bbl':                   safe_int(g(COL_BBL)),
                    'community_board':       safe_int(g(COL_COMM_BOARD)),
                    'x_coord':               safe_int(g(COL_X_COORD)),
                    'y_coord':               safe_int(g(COL_Y_COORD)),
                    'latitude':              safe_float(g(COL_LATITUDE)),
                    'longitude':             safe_float(g(COL_LONGITUDE)),
                    'agency':                g(COL_AGENCY),
                    'agency_name':           g(COL_AGENCY_NAME),
                    'complaint_type':        g(COL_COMPLAINT),
                    'descriptor':            g(COL_DESCRIPTOR),
                    'location_type':         g(COL_LOC_TYPE),
                    'address_type':          g(COL_ADDR_TYPE),
                    'facility_type':         g(COL_FACILITY),
                    'status':                g(COL_STATUS),
                    'open_data_channel':     g(COL_CHANNEL),
                    'borough_name':          g(COL_BOROUGH),
                    'park_borough':          g(COL_PARK_BORO),
                    'additional_details':    g(COL_ADDL_DETAILS),
                    'incident_address':      g(COL_INC_ADDR),
                    'street_name':           g(COL_STREET),
                    'cross_street_1':        g(COL_CROSS1),
                    'cross_street_2':        g(COL_CROSS2),
                    'intersect_street_1':    g(COL_INTERSECT1),
                    'intersect_street_2':    g(COL_INTERSECT2),
                    'city':                  g(COL_CITY),
                    'landmark':              g(COL_LANDMARK),
                    'resolution_desc':       g(COL_RES_DESC),
                    'park_facility':         g(COL_PARK_FAC),
                    'vehicle_type':          g(COL_VEHICLE),
                    'taxi_company_borough':  g(COL_TAXI_BORO),
                    'taxi_pickup':           g(COL_TAXI_PICKUP),
                    'bridge_hwy_name':       g(COL_BRIDGE_NAME),
                    'bridge_hwy_dir':        g(COL_BRIDGE_DIR),
                    'road_ramp':             g(COL_ROAD_RAMP),
                    'bridge_hwy_seg':        g(COL_BRIDGE_SEG),
                    'location':              g(COL_LOCATION),
                }
                records.append(r)
            except Exception:
                continue
    print(f"[Python] loaded {len(records)} records", file=sys.stderr)
    return records


def to_proto(r):
    sr = mini2_pb2.ServiceRecord()
    sr.unique_key              = r['unique_key']
    sr.created_date            = r['created_date']
    sr.closed_date             = r['closed_date']
    sr.due_date                = r['due_date']
    sr.resolution_updated_date = r['resolution_updated_date']
    sr.incident_zip            = r['incident_zip']
    sr.bbl                     = r['bbl']
    sr.community_board         = r['community_board']
    sr.x_coord                 = r['x_coord']
    sr.y_coord                 = r['y_coord']
    sr.latitude                = r['latitude']
    sr.longitude               = r['longitude']
    sr.agency                  = r['agency']
    sr.agency_name             = r['agency_name']
    sr.complaint_type          = r['complaint_type']
    sr.complaint_detail        = r['descriptor']
    sr.location_type           = r['location_type']
    sr.address_type            = r['address_type']
    sr.facility_type           = r['facility_type']
    sr.status                  = r['status']
    sr.open_data_channel       = r['open_data_channel']
    sr.borough_name            = r['borough_name']
    sr.park_borough            = r['park_borough']
    sr.additional_details      = r['additional_details']
    sr.incident_address        = r['incident_address']
    sr.street_name             = r['street_name']
    sr.cross_street_1          = r['cross_street_1']
    sr.cross_street_2          = r['cross_street_2']
    sr.intersect_street_1      = r['intersect_street_1']
    sr.intersect_street_2      = r['intersect_street_2']
    sr.city                    = r['city']
    sr.landmark                = r['landmark']
    sr.resolution_desc         = r['resolution_desc']
    sr.park_facility           = r['park_facility']
    sr.vehicle_type            = r['vehicle_type']
    sr.taxi_company_borough    = r['taxi_company_borough']
    sr.taxi_pickup             = r['taxi_pickup']
    sr.bridge_hwy_name         = r['bridge_hwy_name']
    sr.bridge_hwy_dir          = r['bridge_hwy_dir']
    sr.road_ramp               = r['road_ramp']
    sr.bridge_hwy_seg          = r['bridge_hwy_seg']
    sr.location                = r['location']
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