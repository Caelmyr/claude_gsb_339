"""Smoke tests for the simulation engines, storage and run lifecycle.

Run directly::

    python3 tests/smoke.py

Each check is independent and prints PASS / FAIL; the script exits non-zero on
the first failure so it can be wired into CI or a pre-commit hook.
"""

from __future__ import annotations

import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import models, realtime, report, storage  # noqa: E402
from backend.engine import make_engine  # noqa: E402
from backend.run_manager import manager  # noqa: E402

_ENGINES = ["traffic/ca", "traffic/abm", "ecology/ca", "ecology/abm",
            "epidemic/ca", "epidemic/abm"]


def check(name: str, fn) -> None:
    try:
        fn()
        print(f"PASS  {name}")
    except AssertionError as exc:
        print(f"FAIL  {name}: {exc}")
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL  {name}: {exc}")
        sys.exit(1)


def engines_step() -> None:
    for key in _ENGINES:
        d, m = key.split("/")
        eng = make_engine(d, m, seed=42)
        for _ in range(5):
            eng.step()
        assert eng.step_count == 5, key
        assert len(eng.individuals()) > 0, f"{key} has no individuals"
        stats = eng.stats()
        assert stats, f"{key} produced empty stats"
        snap = eng.snapshot()
        assert snap["step"] == 5
        assert snap["stats"] == stats


def interventions_apply() -> None:
    eng = make_engine("epidemic", "abm", seed=1)
    before = eng.stats()["susceptible"]
    res = eng.apply_intervention({"type": "vaccinate", "params": {"fraction": 1.0}})
    assert res["applied"], res
    assert eng.stats()["susceptible"] == 0
    assert before > 0


def storage_atomic_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as td:
        # Redirect the module's DATA_DIR for this isolated check.
        old = storage.DATA_DIR
        storage.DATA_DIR = td
        try:
            storage.ensure_dirs()
            storage.save_scene({"id": "x", "name": "t", "updated_at": "z"})
            assert storage.load_scene("x")["name"] == "t"
            storage.save_step("r1", 0, {"step": 0, "v": 1})
            storage.save_step("r1", 7, {"step": 7, "v": 2})
            assert storage.load_step("r1", 7)["v"] == 2
            assert storage.list_steps("r1") == [0, 7]
        finally:
            storage.DATA_DIR = old


def run_lifecycle() -> None:
    scene = models.Scene(domain="epidemic", model="abm",
                         config={"n": 200, "width": 300, "height": 300,
                                 "initial_infected": 5})
    meta = manager.create_run(scene, seed=1, snapshot_interval=2)
    rid = meta["id"]
    try:
        r = manager.step(rid, 6)
        assert r["step"] == 6
        series = manager.get_series(rid)
        assert series[0]["step"] == 0 and series[-1]["step"] == 6
        assert len(manager.get_individuals(rid, 6)) == 200
        # snapshot_interval=2 -> full snapshots persisted at 0,2,4,6
        steps = storage.list_steps(rid)
        assert steps == [0, 2, 4, 6], steps
        rpt = report.generate_report(rid)
        assert rpt["steps"] == 7
    finally:
        manager.delete_run(rid)


def broker_coalesce_ordering() -> None:
    b = realtime.EventBroker()
    import time as _time
    # Two runs emit progress bursts; only the newest pending frame per run is
    # kept, and a terminal status is sequenced AFTER all drained progress.
    b.publish_coalesced(realtime.PROGRESS, "rA", {"step": 1})
    b.publish_coalesced(realtime.PROGRESS, "rA", {"step": 2})
    b.publish_coalesced(realtime.PROGRESS, "rB", {"step": 9})
    b.publish(realtime.RUN_STATUS, "rA", {"status": "finished"})
    time.sleep(0.25)  # let the flusher drain anything left

    events = [(t, rid, p) for (_s, t, rid, p) in list(b._ring)]
    types_a = [(t, p.get("status", p.get("step")))
               for (t, rid, p) in events if rid == "rA"]
    assert ("progress", 2) in types_a and ("progress", 1) not in types_a, types_a
    last_prog = max(i for i, (t, _v) in enumerate(types_a) if t == "progress")
    assert types_a[-1] == ("run.status", "finished")
    assert all(i < len(types_a) - 1 for i in [last_prog])

    # Selective subscriptions: rB-only subscriber never sees rA frames.
    sub_b = b.subscribe(run_ids={"rB"})
    sub_global = b.subscribe(global_scope=True)
    b.publish_coalesced(realtime.PROGRESS, "rA", {"step": 3})
    b.publish_coalesced(realtime.PROGRESS, "rB", {"step": 10})
    time.sleep(0.25)
    b_rids = {rid for (_s, _t, rid, _p) in drain(sub_b)}
    assert b_rids == {"rB"}, b_rids
    assert len(drain(sub_global)) >= 2


def broker_replay_and_resync() -> None:
    b = realtime.EventBroker()
    for i in range(1, 6):
        b.publish(realtime.RUN_STATUS, "r1", {"i": i})
    middle = b._ring[2][0]
    # Replaying from inside the window delivers strictly newer frames only.
    sub = b.subscribe(run_ids={"r1"}, since=middle)
    got = drain(sub)
    assert [p["i"] for (_s, _t, _r, p) in got] == [4, 5], got
    # A positive checkpoint older than the oldest retained seq (server restart,
    # or offline longer than the ring window) forces resync, never silent loss.
    b2 = realtime.EventBroker()
    for i in range(1, 6):
        b2.publish(realtime.RUN_STATUS, "r1", {"i": i})
    b2._ring.clear()                       # history before seq 6 is gone
    b2.publish(realtime.RUN_STATUS, "r1", {"i": 6})
    behind = b2.subscribe(run_ids={"r1"}, since=3)
    assert behind.need_resync
    # Checkpoint ahead of a freshly restarted sequence space -> resync too.
    assert b2.subscribe(run_ids={"r1"}, since=999999).need_resync
    # No checkpoint (fresh page) never resyncs; the page's own REST load wins.
    assert not b2.subscribe(global_scope=True).need_resync


def drain(sub, wait=0.3):
    import queue as _q
    out = []
    _deadline = time.time() + wait
    while time.time() < _deadline:
        try:
            out.append(sub.queue.get(timeout=0.05))
        except Exception:  # noqa: BLE001
            break
    return out


def sse_generator_frames() -> None:
    # Exercise realtime.stream() without a web framework: hello frame, live
    # coalesced progress and resync are all plain framing.
    broker = realtime.EventBroker()

    # Fresh subscription -> hello(ready), then live events published after it.
    sub = broker.subscribe(run_ids={"r1"})
    gen = realtime.stream(sub)
    first = parse_frames(next_block(gen))
    assert first[0][0] == "hello" and first[0][1]["reason"] == "ready", first

    broker.publish(realtime.RUN_STATUS, "r1", {"status": "running"})
    status_frames = parse_frames(next_block(gen, timeout=2.0))
    assert any(t == "run.status" and d["status"] == "running"
               for t, d in status_frames), status_frames

    # Live coalesced progress is delivered after the flush tick.
    broker.publish_coalesced(realtime.PROGRESS, "r1", {"step": 4})
    live = parse_frames(next_block(gen, timeout=2.0))
    assert any(t == "progress" and d["step"] == 4 for t, d in live), live

    # Slow consumer flagged for resync gets hello(resync) and nothing stale.
    sub.need_resync = True
    sub.queue.put((broker.current_seq(), "run.status", "r1", {"x": 1}))
    rs = parse_frames(next_block(gen, timeout=2.0))
    assert rs[0][0] == "hello" and rs[0][1]["reason"] == "resync", rs
    assert not any(t == "run.status" for t, _d in rs), rs


def next_block(gen, timeout=1.0):
    """Collect generator bytes until at least one complete event is framed."""
    buf = b""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        buf += next(gen)
        if b"event:" in buf and b"\n\n" in buf:
            return buf
    return buf


def parse_frames(raw):
    import json as _json
    out = []
    for frame in raw.split(b"\n\n"):
        lines = frame.decode().splitlines()
        etype = next((l[7:] for l in lines if l.startswith("event:")), None)
        if etype is None:
            continue
        data = next((l[6:] for l in lines if l.startswith("data:")), "{}")
        out.append((etype, _json.loads(data)))
    return out


def sse_endpoint_live() -> None:
    # Full HTTP round-trip; skipped where Flask isn't installed (e.g. a bare
    # CI image), the framing path is covered by sse_generator_frames.
    import importlib.util
    if importlib.util.find_spec("flask") is None:
        print("SKIP  SSE HTTP round-trip (flask not installed)")
        return
    from backend.app import create_app
    app = create_app()
    client = app.test_client()

    scene = models.Scene(domain="epidemic", model="abm",
                         config={"n": 60, "width": 200, "height": 200})
    meta = manager.create_run(scene, seed=3, snapshot_interval=1)
    rid = meta["id"]
    try:
        with client.get(f"/api/stream?run={rid}") as resp:
            assert resp.status_code == 200
            assert resp.mimetype == "text/event-stream"
            it = resp.response
            assert next_sse(it)[0] == "hello"

            import threading as _t
            th = _t.Thread(target=manager.run_batch, args=(rid, 20),
                           kwargs={"keep_engine": True}, daemon=True)
            th.start()
            saw = {"progress": 0, "run.status": 0}
            while th.is_alive() or saw["run.status"] < 2:
                ev_type, data = next_sse(it)
                if ev_type in saw:
                    saw[ev_type] += 1
                if ev_type == "run.status" and data.get("status") in (
                        "finished", "stopped"):
                    break
            th.join(timeout=5)
            assert saw["progress"] >= 1 and saw["run.status"] >= 1, saw

        # A stream without any subscription scope is a client error.
        assert client.get("/api/stream").status_code == 400
    finally:
        manager.delete_run(rid)


def next_sse(it, max_chunks=500):
    """Read one complete event-bearing SSE frame (blank-line terminated).

    Non-event frames (the initial ``retry:`` hint, ``: ping`` heartbeats) are
    skipped.  Frames may span chunk boundaries, so a small carry buffer is kept.
    """
    import json as _json
    carry = b""
    while True:
        if b"\n\n" not in carry:
            chunk = next(it)
            carry += chunk
        while b"\n\n" in carry:
            frame, carry = carry.split(b"\n\n", 1)
            lines = frame.decode().splitlines()
            ev_type = next((l[7:] for l in lines if l.startswith("event:")), None)
            if ev_type is None:
                continue  # retry hint / heartbeat comment
            data = next((l[6:] for l in lines if l.startswith("data:")), "{}")
            return ev_type, _json.loads(data)
        max_chunks -= 1
        if max_chunks <= 0:
            raise AssertionError("no complete SSE event frame")


def main() -> None:
    check("six engines step and snapshot", engines_step)
    check("interventions apply", interventions_apply)
    check("atomic sharded storage", storage_atomic_roundtrip)
    check("run lifecycle + report", run_lifecycle)
    check("broker coalesce + ordering + filters", broker_coalesce_ordering)
    check("broker replay + resync", broker_replay_and_resync)
    check("SSE framing / hello / resync", sse_generator_frames)
    check("SSE endpoint streams live frames", sse_endpoint_live)
    print("\nall smoke tests passed")


if __name__ == "__main__":
    main()
