#!/usr/bin/env python3
"""
Lab 4 Starter — Coordinator (2PC/3PC) (HTTP, standard library only)
===================================================================

Endpoints (JSON):
- POST /tx/start   {"txid":"TX1","op":{"type":"SET","key":"x","value":"5"}, "protocol":"2PC"|"3PC"}
- GET  /status

Participants are addressed by base URL (e.g., http://10.0.1.12:8001).

Failure injection:
- Kill the coordinator between phases to demonstrate blocking (2PC).
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
from typing import Dict, Any, List, Optional, Tuple
import os  # for fsync

lock = threading.Lock()

NODE_ID: str = ""
PORT: int = 8000
PARTICIPANTS: List[str] = []
TIMEOUT_S: float = 2.0
RETRIES: int = 3  # for decision propagation

TX: Dict[str, Dict[str, Any]] = {}

WAL_PATH: str = "/tmp/coordinator.wal"  # YOUR CODE HERE: persist decisions

def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")

def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def post_json(url: str, payload: dict, timeout: float = TIMEOUT_S) -> Tuple[int, dict]:
    data = jdump(payload)
    req = request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            return resp.status, jload(resp.read())
    except Exception as e:
        return 500, {"error": str(e)}

def wal_append(line: str) -> None:
    with open(WAL_PATH, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")
        f.flush()
        os.fsync(f.fileno())

def wal_replay() -> None:
    if not os.path.exists(WAL_PATH):
        return
    with open(WAL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(maxsplit=2)
            if len(parts) < 2:
                continue
            txid, decision = parts[0], parts[1]
            with lock:
                rec = TX.get(txid)
                if rec and rec["decision"] is None:
                    rec["decision"] = decision
                    rec["state"] = f"{decision}_SENT"
    print(f"[{NODE_ID}] Coordinator WAL replay done, tx={TX}")

def propagate_decision(txid: str, decision: str) -> None:
    endpoint = "/commit" if decision == "COMMIT" else "/abort"
    for attempt in range(1, RETRIES + 1):
        success = True
        for p in PARTICIPANTS:
            status, resp = post_json(p.rstrip("/") + endpoint, {"txid": txid})
            if status != 200:
                success = False
                print(f"[{NODE_ID}] Retry {attempt}/{RETRIES} failed for {p}")
        if success:
            break
        time.sleep(0.5 * attempt)  # backoff

def two_pc(txid: str, op: dict) -> dict:
    with lock:
        TX[txid] = {
            "txid": txid, "protocol": "2PC", "state": "PREPARE_SENT",
            "op": op, "votes": {}, "decision": None,
            "participants": list(PARTICIPANTS), "ts": time.time()
        }

    votes = {}
    all_yes = True

    for p in PARTICIPANTS:
        _, resp = post_json(p.rstrip("/") + "/prepare", {"txid": txid, "op": op})
        vote = str(resp.get("vote", "NO")).upper()
        votes[p] = vote
        if vote != "YES":
            all_yes = False

    decision = "COMMIT" if all_yes else "ABORT"

    print(f"[{NODE_ID}] TX {txid} → all YES, sleeping 15 seconds before sending COMMIT...")
    time.sleep(15)

    with lock:
        TX[txid]["votes"] = votes
        TX[txid]["decision"] = decision
        TX[txid]["state"] = f"{decision}_SENT"
    wal_append(f"{txid} {decision}")  # log decision

    # YOUR CODE HERE: retries in propagation
    propagate_decision(txid, decision)

    return {"ok": True, "txid": txid, "decision": decision}

def three_pc(txid: str, op: dict) -> dict:
    """
    Three-Phase Commit (3PC) implementation
    Фазы:
    1. CanCommit (аналог Prepare в 2PC) — участники голосуют "готовы ли"
    2. PreCommit — участники подтверждают, что все готовы → переход в PRECOMMIT
    3. DoCommit / DoAbort — финальное выполнение
    """
    with lock:
        TX[txid] = {
            "txid": txid,
            "protocol": "3PC",
            "state": "CAN_COMMIT_SENT",
            "op": op,
            "votes_can_commit": {},
            "precommit_ok": {},
            "decision": None,
            "participants": list(PARTICIPANTS),
            "ts": time.time()
        }
        log_msg = f"TX {txid} 3PC started → CAN_COMMIT_SENT"
        wal_append(f"{txid} START_3PC {json.dumps(op)}")

    # ───────────────────────────────────────────────
    # Phase 1: CanCommit (все должны сказать YES)
    # ───────────────────────────────────────────────
    votes = {}
    all_yes = True

    for p in PARTICIPANTS:
        try:
            status, resp = post_json(
                p.rstrip("/") + "/can_commit",
                {"txid": txid, "op": op},
                timeout=1.5
            )
            vote = str(resp.get("vote", "NO")).upper()
            votes[p] = vote
            log(f"CanCommit → {p} ответил {vote}")
            if vote != "YES":
                all_yes = False
        except Exception as e:
            votes[p] = "NO_ERROR"
            all_yes = False
            log(f"CanCommit → {p} ошибка: {e}")

    with lock:
        TX[txid]["votes_can_commit"] = votes

    if not all_yes:
        decision = "ABORT"
        with lock:
            TX[txid]["decision"] = decision
            TX[txid]["state"] = "ABORT_SENT"
        wal_append(f"{txid} ABORT after CanCommit")
        propagate_decision(txid, decision)
        log(f"3PC {txid} → ABORT (не все YES в CanCommit)")
        return {"ok": True, "txid": txid, "decision": decision}

    # ───────────────────────────────────────────────
    # Phase 2: PreCommit
    # ───────────────────────────────────────────────
    precommit_success = True
    precommit_responses = {}

    for p in PARTICIPANTS:
        try:
            status, resp = post_json(
                p.rstrip("/") + "/precommit",
                {"txid": txid},
                timeout=1.5
            )
            if status == 200:
                precommit_responses[p] = "OK"
            else:
                precommit_responses[p] = f"ERROR_{status}"
                precommit_success = False
            log(f"PreCommit → {p} статус {status}")
        except Exception as e:
            precommit_responses[p] = f"EXCEPTION_{str(e)}"
            precommit_success = False
            log(f"PreCommit → {p} исключение: {e}")

    with lock:
        TX[txid]["precommit_ok"] = precommit_responses

    if not precommit_success:
        decision = "ABORT"
    else:
        decision = "COMMIT"

    with lock:
        TX[txid]["decision"] = decision
        TX[txid]["state"] = f"{decision}_SENT"

    wal_append(f"{txid} {decision} after PreCommit")
    log(f"3PC {txid} → {decision} (PreCommit {'успешен' if precommit_success else 'провал'})")

    # ───────────────────────────────────────────────
    # Phase 3: DoCommit / DoAbort
    # ───────────────────────────────────────────────
    propagate_decision(txid, decision)

    return {
        "ok": True,
        "txid": txid,
        "decision": decision,
        "can_commit_votes": votes,
        "precommit_responses": precommit_responses
    }

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {"ok": True, "node": NODE_ID, "port": PORT, "participants": PARTICIPANTS, "tx": TX})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = jload(raw)
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        if self.path == "/tx/start":
            txid = str(body.get("txid", "")).strip()
            op = body.get("op", None)
            protocol = str(body.get("protocol", "2PC")).upper()

            if not txid or not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "txid and op required"})
                return
            if protocol not in ("2PC", "3PC"):
                self._send(400, {"ok": False, "error": "protocol must be 2PC or 3PC"})
                return

            if protocol == "2PC":
                result = two_pc(txid, op)
            else:
                result = three_pc(txid, op)

            self._send(200, result)
            return

        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        return

def main():
    global NODE_ID, PORT, PARTICIPANTS
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="COORD")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--participants", required=True, help="Comma-separated participant base URLs[](http://IP:PORT)")
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    PARTICIPANTS = [p.strip() for p in args.participants.split(",") if p.strip()]

    wal_replay()  # replay decisions on startup

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[{NODE_ID}] Coordinator listening on {args.host}:{args.port} participants={PARTICIPANTS}")
    server.serve_forever()

if __name__ == "__main__":
    main()