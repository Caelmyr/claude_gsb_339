"""Server-Sent-Events broker with bounded replay and resumable subscriptions.

Each stream is addressed by a topic (for example ``run:<id>:progress``).  Events
on a topic have a gap-free per-topic ``seq`` while the server process is alive.
Recent events are kept in memory so a short network failure can be resumed with
``Last-Event-ID``.  If the cursor is too old or the server has restarted, the
stream sends a ``hello`` event requesting a normal REST resync; clients then
re-apply only newer buffered events.  This gives at-least-once transport with
idempotent, ordered client handling rather than assuming push cannot repeat.
"""

from __future__ import annotations

import base64
import json
import queue
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Keep enough history for brief disconnects.  Intervention events are durable and
# can be reconstructed with REST, while progress is compact and needs a larger
# replay window for long batches.
_LIMITS = {
    "run.status": 50,
    "run.created": 50,
    "run.deleted": 50,
    "run.progress": 5000,
    "run.event": 10000,
    "experiment.status": 100,
}
_TOPIC_LIMITS = {
    "progress": 5000,
    "events": 10000,
}
_DEFAULT_LIMIT = 1000
_QUEUE_SIZE = 512


def encode_cursor(cursors: Dict[str, int]) -> str:
    raw = json.dumps(cursors, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(value: Optional[str]) -> Dict[str, int]:
    if not value:
        return {}
    try:
        pad = "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(value + pad).decode("utf-8")
        data = json.loads(raw)
        if isinstance(data, dict):
            return {str(k): int(v) for k, v in data.items() if int(v) >= 0}
    except (ValueError, TypeError, UnicodeDecodeError):
        pass
    return {}


class EventBroker:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._seq: Dict[str, int] = {}
        self._global_seq = 0
        self._subscribers: Dict[str, "set[queue.Queue]"] = {}
        self._hydrated: set = set()
        self._aliases: Dict[str, "set[str]"] = {}

    # ------------------------------------------------------------------ #
    # Publishing
    # ------------------------------------------------------------------ #
    def publish(self, topic: str, event_type: str, data: Dict[str, Any],
                seq: Optional[int] = None) -> Dict[str, Any]:
        """Publish one event to ``topic`` and all of its alias targets."""
        with self._lock:
            event = self._append(topic, event_type, data, seq)
            targets = list(self._aliases.get(topic, ()))
        for target in targets:
            # Aliased events preserve their original type and sequence.  The
            # target topic's own cursor still sees an ordered copied stream.
            self._copy(target, event)
        return event

    def add_alias(self, source: str, target: str) -> None:
        """Copy current and future ``source`` events into ``target``."""
        with self._lock:
            self._ensure_topic(target)
            for event in list(self._history.get(source, ())):
                self._append(target, event["type"], event["data"], None)
            self._aliases.setdefault(source, set()).add(target)

    def _append(self, topic: str, event_type: str, data: Dict[str, Any],
                seq: Optional[int]) -> Dict[str, Any]:
        with self._lock:
            self._ensure_topic(topic)
            if seq is None:
                seq = self._seq[topic] + 1
            self._global_seq += 1
            event = {
                "type": event_type,
                "seq": int(seq),
                "order": self._global_seq,
                "time": time.time(),
                "data": dict(data),
            }
            self._seq[topic] = max(self._seq[topic], int(event["seq"]))
            history = self._history[topic]
            history.append(event)
            topic_kind = topic.rsplit(":", 1)[-1]
            limit = (_TOPIC_LIMITS.get(topic_kind)
                     or _LIMITS.get(event["type"], _DEFAULT_LIMIT))
            if len(history) > limit:
                del history[:-limit]
            for q in self._subscribers.get(topic, set()).copy():
                _put_event(q, topic, event)
            return event

    def _copy(self, target: str, event: Dict[str, Any]) -> None:
        with self._lock:
            self._append(target, event["type"], event["data"], None)

    def _ensure_topic(self, topic: str) -> None:
        self._history.setdefault(topic, [])
        self._seq.setdefault(topic, 0)
        self._subscribers.setdefault(topic, set())

    def reset_topics(self, topics: Iterable[str]) -> None:
        """Clear volatile streams when a run is reset to step zero."""
        with self._lock:
            for topic in topics:
                self._ensure_topic(topic)
                self._history[topic] = []
                self._seq[topic] = 0

    # ------------------------------------------------------------------ #
    # Durable hydration after a process restart
    # ------------------------------------------------------------------ #
    def hydrate_run(self, run_id: str) -> None:
        status_topic = f"run:{run_id}:status"
        events_topic = f"run:{run_id}:events"
        progress_topic = f"run:{run_id}:progress"
        with self._lock:
            if status_topic in self._hydrated:
                return
            self._hydrated.add(status_topic)
            # Import lazily so this module does not depend on storage details.
            from . import storage
            meta = storage.load_run_meta(run_id)
            events = storage.load_events(run_id)
            self._ensure_topic(status_topic)
            self._ensure_topic(events_topic)
            self._ensure_topic(progress_topic)
            if meta is not None:
                revision = int(meta.get("status_revision", 0))
                if revision > 0:
                    self._append(status_topic, "run.status",
                                 _status_payload(meta, "resumed"), revision)
            for event in events:
                seq = int(event.get("seq", 0))
                if seq > 0:
                    self._append(events_topic, "run.event", event, seq)

    def hydrate_experiment(self, exp_id: str) -> None:
        topic = f"exp:{exp_id}:stream"
        with self._lock:
            if topic in self._hydrated:
                return
            self._hydrated.add(topic)
            from . import storage
            exp = storage.load_experiment(exp_id)
            self._ensure_topic(topic)
            if exp is not None:
                seq = int(exp.get("status_revision", 0))
                if seq > 0:
                    self._append(topic, "experiment.status",
                                 _experiment_payload(exp, "resumed"), seq)

    # ------------------------------------------------------------------ #
    # Subscriptions
    # ------------------------------------------------------------------ #
    def subscribe_multi(self, topics: Iterable[str],
                        cursors: Dict[str, int]
                        ) -> Tuple["queue.Queue", List[Tuple[str, Dict[str, Any]]], bool]:
        """Subscribe to several topics and atomically return missed history."""
        topics = list(dict.fromkeys(topics))
        q: "queue.Queue[Tuple[Optional[str], Any]]" = queue.Queue(_QUEUE_SIZE)
        replay_pairs: List[Tuple[str, Dict[str, Any]]] = []
        needs_resync = False
        with self._lock:
            for topic in topics:
                self._ensure_topic(topic)
                cursor = int(cursors.get(topic, 0))
                history = self._history[topic]
                if cursor:
                    latest = self._seq[topic]
                    oldest = history[0]["seq"] if history else 1
                    if cursor < oldest or cursor > latest:
                        needs_resync = True
                    replay_pairs.extend((topic, e) for e in history
                                  if int(e["seq"]) > cursor)
                else:
                    needs_resync = True
                self._subscribers[topic].add(q)
        replay = sorted(replay_pairs, key=lambda item: item[1].get("order", 0))
        return q, replay, needs_resync

    def unsubscribe_multi(self, topics: Iterable[str],
                          q: "queue.Queue") -> None:
        with self._lock:
            for topic in topics:
                self._subscribers.get(topic, set()).discard(q)

    def get_nowait(self, q: "queue.Queue") -> Optional[Tuple[Optional[str], Any]]:
        try:
            return q.get_nowait()
        except queue.Empty:
            return None


def _put_event(q: "queue.Queue", topic: str, event: Dict[str, Any]) -> None:
    try:
        q.put_nowait((topic, event))
    except queue.Full:
        # A stalled browser tab must not grow server memory forever.  Drop the
        # oldest queued event and ask it to resync from authoritative REST data.
        try:
            q.get_nowait()
        except queue.Empty:
            pass
        try:
            q.put_nowait((None, {"__resync__": True, "reason": "backlog overflow"}))
        except queue.Full:
            pass


def _status_payload(meta: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return {**meta, "reason": reason}


def _experiment_payload(exp: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return {**exp, "reason": reason}


broker = EventBroker()
