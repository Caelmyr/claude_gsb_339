"""Run lifecycle management: real-time stepping, batch runs, interventions.

A :class:`RunManager` owns the in-memory engines for the current server
process.  Every mutation funnels through :mod:`backend.storage` so the
atomic-write / time-step-sharding guarantees hold whether a step comes from the
UI or a background batch.  Engines are kept in memory while a run exists in
this process so a run can be stepped interactively; batch runs that belong to a
comparison experiment can drop their engine on completion to bound memory.

Concurrency: each run has its own re-entrant lock, so long batch runs on one
scene never block interactive stepping on another.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from . import models, storage, util
from .engine import make_engine
from .engine.base import Engine
from .events import broker


class RunManager:
    def __init__(self) -> None:
        self._engines: Dict[str, Engine] = {}
        self._locks: Dict[str, threading.RLock] = {}
        self._abort: set = set()
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #
    def _lock_for(self, run_id: str) -> threading.RLock:
        with self._lock:
            return self._locks.setdefault(run_id, threading.RLock())

    def _engine(self, run_id: str) -> Optional[Engine]:
        with self._lock:
            return self._engines.get(run_id)

    def _require(self, run_id: str) -> tuple:
        eng = self._engine(run_id)
        meta = storage.load_run_meta(run_id)
        if meta is None:
            raise KeyError(f"run not found: {run_id}")
        return eng, meta

    def _status_payload(self, meta: Dict[str, Any],
                        engine: Optional[Engine], reason: str) -> Dict[str, Any]:
        payload = dict(meta)
        payload["reason"] = reason
        if engine is not None:
            payload["stats"] = engine.stats()
        return payload

    def _save_status(self, meta: Dict[str, Any], engine: Optional[Engine],
                     reason: str) -> Dict[str, Any]:
        """Persist a status/meta revision and publish the ordered status event."""
        meta["status_revision"] = int(meta.get("status_revision", 0)) + 1
        meta["updated_at"] = util.now_iso()
        storage.save_run_meta(meta)
        payload = self._status_payload(meta, engine, reason)
        broker.publish(f"run:{meta['id']}:status", "run.status", payload,
                       int(meta["status_revision"]))
        broker.publish("runs:status", "run.status", payload)
        return payload

    def _append_event(self, run_id: str, engine: Engine,
                      event: Dict[str, Any]) -> Dict[str, Any]:
        events = storage.load_events(run_id)
        seq = max([int(e.get("seq", 0)) for e in events] or [0]) + 1
        item = {
            "id": f"evt_{run_id}_{seq}",
            "seq": seq,
            "step": engine.step_count,
            "time": util.now_iso(),
            **event,
        }
        events.append(item)
        storage.save_events(run_id, events)
        broker.publish(f"run:{run_id}:events", "run.event", item, seq)
        return item

    def _publish_progress(self, meta: Dict[str, Any], engine: Engine,
                          series: List[Dict[str, Any]],
                          last_published_step: int,
                          force: bool = False,
                          *, now: Optional[float] = None,
                          last_time: float = 0.0
                          ) -> Tuple[int, float]:
        """Publish coalesced progress at roughly 10 updates/second.

        Suppressed updates are not lost: the next emitted message carries every
        compact series row accumulated since the prior message.  The call is
        deliberately cheap when no progress slot is due.
        """
        current = engine.step_count
        current_time = now if now is not None else time.monotonic()
        due = force or current_time - last_time >= 0.1
        if not due:
            return last_published_step, last_time
        rows = [row for row in series
                if int(row["step"]) > last_published_step]
        if not rows and current == last_published_step:
            return last_published_step, last_time
        data = {
            "run_id": meta["id"],
            "step": current,
            "target_step": int(meta.get("target_step", 0) or 0),
            "stats": engine.stats(),
            "rows": rows,
            "snapshot_available": current % int(meta["snapshot_interval"]) == 0,
        }
        broker.publish(f"run:{meta['id']}:progress", "run.progress", data)
        return current, current_time

    # ------------------------------------------------------------------ #
    # Create
    # ------------------------------------------------------------------ #
    def create_run(self, scene: models.Scene, name: Optional[str] = None,
                   seed: Optional[int] = None,
                   snapshot_interval: int = 1,
                   publish: bool = True,
                   alias_topics: Optional[List[str]] = None
                   ) -> Dict[str, Any]:
        config = models.resolve_config(scene)
        if seed is None:
            seed = int(config.get("seed", 0))
        engine = make_engine(scene.domain, scene.model, config=config, seed=seed)
        run_id = util.new_id("run")
        now = util.now_iso()
        meta: Dict[str, Any] = {
            "id": run_id,
            "name": name or f"{scene.name} · 运行",
            "scene_id": scene.id,
            "scene_name": scene.name,
            "domain": scene.domain,
            "model": scene.model,
            "config": config,
            "interventions": [{**i, "applied": False}
                              for i in scene.interventions],
            "snapshot_interval": max(1, int(snapshot_interval)),
            "status": "ready",
            "current_step": 0,
            "target_step": 0,
            "total_steps": 0,
            "seed": seed,
            "status_revision": 0,
            "created_at": now,
            "updated_at": now,
        }
        storage.create_run_dir(run_id)
        storage.save_step(run_id, 0, engine.snapshot())
        storage.save_series(run_id, [{"step": 0, **engine.stats()}])
        storage.save_events(run_id, [])
        # Save once before subscribing aliases; _save_status performs the first
        # numbered revision after any experiment alias has been installed.
        storage.save_run_meta(meta)
        with self._lock:
            self._engines[run_id] = engine
        if alias_topics:
            for kind in ("status", "progress", "events"):
                topic = f"run:{run_id}:{kind}"
                for target in alias_topics:
                    broker.add_alias(topic, target)
        if publish:
            self._save_status(meta, engine, "created")
            broker.publish("runs:created", "run.created", dict(meta))
        elif alias_topics:
            # Experiments subscribe before the run starts, so make its initial
            # ready state visible without also emitting a global list event.
            self._save_status(meta, engine, "created")
        return meta

    # ------------------------------------------------------------------ #
    # Stepping
    # ------------------------------------------------------------------ #
    def _apply_due(self, run_id: str, engine: Engine,
                   meta: Dict[str, Any], step: int) -> None:
        """Apply any scheduled intervention whose ``at_step`` has been reached."""
        changed = False
        for itv in meta["interventions"]:
            if itv.get("applied"):
                continue
            if int(itv.get("at_step", 0)) <= step:
                result = engine.apply_intervention(itv)
                itv["applied"] = True
                self._append_event(run_id, engine, {
                    "type": itv["type"],
                    "params": itv.get("params", {}),
                    "scheduled": True,
                    "result": result,
                })
                changed = True
        if changed:
            self._save_status(meta, engine, "intervention")

    def step(self, run_id: str, n: int = 1) -> Dict[str, Any]:
        """Advance ``n`` steps and return the current snapshot + stats."""
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            if engine is None:
                raise RuntimeError("该运行未载入内存（服务器重启后不可续跑），请重开新运行")
            if meta["status"] in ("finished", "stopped"):
                meta["status"] = "ready"
                meta["target_step"] = 0
                self._save_status(meta, engine, "resumed")
            for _ in range(int(n)):
                self._apply_due(run_id, engine, meta, engine.step_count)
                engine.step()
                meta["current_step"] = engine.step_count
                row = {"step": engine.step_count, **engine.stats()}
                storage.append_series(run_id, row)
                if engine.step_count % meta["snapshot_interval"] == 0:
                    storage.save_step(run_id, engine.step_count, engine.snapshot())
                self._publish_progress(meta, engine, [row],
                                       engine.step_count - 1, force=True)
            meta["updated_at"] = util.now_iso()
            storage.save_run_meta(meta)
            return {"step": engine.step_count, "stats": engine.stats(),
                    "snapshot": engine.snapshot()}

    def run_batch(self, run_id: str, steps: int,
                  snapshot_interval: Optional[int] = None,
                  keep_engine: bool = True) -> Dict[str, Any]:
        """Run ``steps`` steps to completion, returning final stats.

        Series rows are accumulated in memory and flushed periodically (and at
        the end) so the per-step write cost stays O(1) amortised even for very
        long runs.
        """
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            if engine is None:
                raise RuntimeError("该运行未载入内存，请重开新运行")
            if snapshot_interval is not None:
                meta["snapshot_interval"] = max(1, int(snapshot_interval))
            meta["status"] = "running"
            meta["target_step"] = int(meta.get("current_step", 0)) + int(steps)
            self._save_status(meta, engine, "batch_started")

            series = storage.load_series(run_id)
            self._abort.discard(run_id)
            last_published_step = engine.step_count
            last_progress_time = 0.0
            for _ in range(int(steps)):
                if run_id in self._abort:
                    break
                self._apply_due(run_id, engine, meta, engine.step_count)
                engine.step()
                meta["current_step"] = engine.step_count
                row = {"step": engine.step_count, **engine.stats()}
                series.append(row)
                if engine.step_count % meta["snapshot_interval"] == 0:
                    storage.save_step(run_id, engine.step_count, engine.snapshot())
                if engine.step_count % 50 == 0:
                    storage.save_series(run_id, series)
                    stored = storage.load_run_meta(run_id) or {}
                    if int(stored.get("status_revision", 0)) <= int(
                            meta.get("status_revision", 0)):
                        meta["updated_at"] = util.now_iso()
                        storage.save_run_meta(meta)
                last_published_step, last_progress_time = self._publish_progress(
                    meta, engine, series, last_published_step,
                    last_time=last_progress_time)

            storage.save_series(run_id, series)
            final_snapshot = engine.snapshot()
            storage.save_step(run_id, engine.step_count, final_snapshot)
            meta["status"] = "stopped" if run_id in self._abort else "finished"
            meta["target_step"] = 0
            stored = storage.load_run_meta(run_id) or {}
            meta["status_revision"] = max(
                int(meta.get("status_revision", 0)),
                int(stored.get("status_revision", 0)))
            self._save_status(meta, engine,
                              "stopped" if run_id in self._abort else "finished")
            self._publish_progress(meta, engine, series,
                                   last_published_step, force=True)
            self._abort.discard(run_id)

            if not keep_engine:
                with self._lock:
                    self._engines.pop(run_id, None)
            return {"step": engine.step_count, "stats": engine.stats(),
                    "snapshot": final_snapshot}

    # ------------------------------------------------------------------ #
    # Control
    # ------------------------------------------------------------------ #
    def pause(self, run_id: str) -> Dict[str, Any]:
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            meta["status"] = "paused"
            return self._save_status(meta, engine, "paused")

    def resume(self, run_id: str) -> Dict[str, Any]:
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            meta["status"] = "ready"
            meta["target_step"] = 0
            return self._save_status(meta, engine, "resumed")

    def stop(self, run_id: str) -> Dict[str, Any]:
        with self._lock:
            self._abort.add(run_id)
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            if meta.get("status") == "running":
                meta["status"] = "stopping"
                return self._save_status(meta, engine, "stop_requested")
            meta["status"] = "stopped"
            return self._save_status(meta, engine, "stopped")

    def mark_error(self, run_id: str, exc: Exception) -> Dict[str, Any]:
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            with self._lock:
                self._abort.discard(run_id)
            meta["status"] = "error"
            meta["error"] = str(exc)
            meta["target_step"] = 0
            return self._save_status(meta, engine, "error")

    def reset(self, run_id: str) -> Dict[str, Any]:
        with self._lock_for(run_id):
            _, meta = self._require(run_id)
            seed = meta.get("seed", 0)
            engine = make_engine(meta["domain"], meta["model"],
                                 config=meta["config"], seed=seed)
            with self._lock:
                self._engines[run_id] = engine
                self._abort.discard(run_id)
            for itv in meta["interventions"]:
                itv["applied"] = False
            meta["current_step"] = 0
            meta["target_step"] = 0
            meta["status"] = "ready"
            initial_series = [{"step": 0, **engine.stats()}]
            storage.save_step(run_id, 0, engine.snapshot())
            storage.save_series(run_id, initial_series)
            storage.save_events(run_id, [])
            broker.reset_topics([f"run:{run_id}:events",
                                 f"run:{run_id}:progress"])
            self._save_status(meta, engine, "reset")
            broker.publish(f"run:{run_id}:progress", "run.progress", {
                "run_id": run_id,
                "step": 0,
                "target_step": 0,
                "stats": engine.stats(),
                "rows": initial_series,
                "snapshot_available": True,
                "reset": True,
            })
            return meta

    def delete_run(self, run_id: str) -> bool:
        with self._lock_for(run_id):
            with self._lock:
                self._engines.pop(run_id, None)
                self._locks.pop(run_id, None)
                self._abort.discard(run_id)
            deleted = storage.delete_run(run_id)
            if deleted:
                broker.publish("runs:deleted", "run.deleted", {"id": run_id})
            return deleted

    # ------------------------------------------------------------------ #
    # Interventions
    # ------------------------------------------------------------------ #
    def apply_intervention(self, run_id: str,
                           itv: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            if engine is None:
                raise RuntimeError("该运行未载入内存，请重开新运行")
            result = engine.apply_intervention(itv)
            self._append_event(run_id, engine, {
                "type": itv["type"],
                "params": itv.get("params", {}),
                "scheduled": False,
                "result": result,
            })
            self._save_status(meta, engine, "intervention")
            return result

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #
    def status(self, run_id: str) -> Dict[str, Any]:
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            return self._status_payload(meta, engine, "state")

    def get_snapshot(self, run_id: str, step: Optional[int] = None) -> Dict[str, Any]:
        with self._lock_for(run_id):
            engine, meta = self._require(run_id)
            if step is None:
                step = meta["current_step"]
            if engine is not None and int(step) == engine.step_count:
                return engine.snapshot()
            snap = storage.load_step(run_id, int(step))
            if snap is None:
                raise KeyError(f"snapshot not found: step {step}")
            return snap

    def get_series(self, run_id: str) -> List[Dict[str, Any]]:
        with self._lock_for(run_id):
            self._require(run_id)
            return storage.load_series(run_id)

    def get_events(self, run_id: str) -> List[Dict[str, Any]]:
        with self._lock_for(run_id):
            self._require(run_id)
            return storage.load_events(run_id)

    def get_individuals(self, run_id: str, step: Optional[int] = None) -> List[Dict[str, Any]]:
        return self.get_snapshot(run_id, step).get("individuals", [])


# Global singleton used by the Flask app.
manager = RunManager()
