# Lab 4 Starter Templates — Distributed Transactions (2PC / 3PC) (Python)

Minimal EC2-friendly templates (standard library only).

## Files
- `coordinator.py`
- `participant.py`
- `client.py`

## Run example (3 nodes)
Participants:
```bash
python3 participant.py --id B --port 8001 --wal /tmp/participant_B.wal
python3 participant.py --id C --port 8002 --wal /tmp/participant_C.wal
```

Coordinator:
```bash
python3 coordinator.py --id COORD --port 8000 --participants http://<IP-B>:8001,http://<IP-C>:8002
```

Start a 2PC transaction:
```bash
python3 client.py --coord http://<COORD-IP>:8000 start TX1 2PC SET x 5
```

Start a 3PC transaction (bonus):
```bash
python3 client.py --coord http://<COORD-IP>:8000 start TX2 3PC SET y 9
```

## Failure experiments
- Kill coordinator after PREPARE (2PC blocking) and explain participant READY state.
- Kill participant before it votes; coordinator should ABORT by timeout (in this starter, timeout manifests as NO_TIMEOUT vote).

## Where to add code (# YOUR CODE HERE)
- WAL replay on participant startup
- fsync durability in WAL writes
- retries + decision logging in coordinator
- 3PC termination logic (bonus)
