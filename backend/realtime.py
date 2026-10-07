"""Real-time push hub: an in-process pub/sub broker fronted by an SSE stream.

Why not "just swap polling for push"
------------------------------------
The UI needs four guarantees that a naive fan-out socket does not provide:

1. **Selective subscriptions.** A page looking at run A must never receive run
   B's frames.  Each subscriber carries its own filter (a set of run ids, or a
   global flag) and matching happens at fan-out time.
2. **Ordering, no loss, no duplicates.** Every event gets a process-wide,
   monotonically increasing sequence number assigned in publication order.
   Clients remember the last id they handled; after a network blip the
   ``EventSource`` reconnects (browsers send ``Last-Event-ID``) and the broker
   replays whatever the subscriber missed from an in-memory ring buffer.  The
   client applies idempotent de-duplication on top.
3. **Burst coalescing.** Progress during a long batch can be produced far
   faster than any page renders.  ``progress`` events for one run are
   coalesced — only the newest pending frame per run survives — and a single
   flusher thread hands them out on a fixed ~100 ms tick.  Status/intervention
   events are never coalesced and are only released after pending progress, so
   a ``finished`` frame can never overtake the last progress frame.
4. **Recovery beyond the buffer.** If the client was offline too long (or the
   server restarted) and the gap is no longer in the ring, the broker tells it
   to ``resync``: re-fetch authoritative state over REST, which is exactly
   what a fresh page load does.  Slow consumers whose queue fills trigger the
   same path instead of growing memory without bound.

Transport is Server-Sent Events: one long-lived HTTP response per page tab,
built into browsers (``EventSource``: auto-reconnect + ``Last-Event-ID``),
proxies cleanly, and needs no third-party dependency.
"""

from __future__ import annotations

import collections
import json
import queue
import threading
from typing import Any, Deque, Dict, Optional, Set, Tuple

# Event types carried on the wire.
PROGRESS = "progress"          # coalescable per-run step/stats frame
RUN_STATUS = "run.status"      # lifecycle transitions (running/finished/...)
RUN_EVENT = "run.event"        # interventions applied (manual or scheduled)
RUN_CREATED = "run.created"    # a new run appeared
RUN_DELETED = "run.deleted"    # a run was removed
EXPERIMENT_STATUS = "experiment.status"

# Events that describe one run and therefore obey run-id subscriptions.
RUN_SCOPED_TYPES = frozenset({PROGRESS, RUN_STATUS, RUN_EVENT,
                              RUN_CREATED, RUN_DELETED})
# Events that must always be delivered in order and are safe to coalesce.
COALESCABLE_TYPES = frozenset({PROGRESS})

_FLUSH_INTERVAL = 0.1          # progress frames leave at most this often
_HEARTBEAT_INTERVAL = 15.0     # SSE comment ping, also bounds dead-peer detect
_RING_MAX = 2048               # replay window (events)
_QUEUE_MAX = 512               # per-subscriber backlog before forcing resync


class EventBroker:
    """Thread-safe fan-out broker with coalescing and a replay ring."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._seq = 0
        # Bounded ring of (seq, type, run_id, payload) for reconnect replay.
        self._ring: Deque[Tuple[int, str, Optional[str], Dict[str, Any]]] = \
            collections.deque(maxlen=_RING_MAX)
        # Pending coalescable frames, keyed by run id; flushed by the timer.
        self._pending: Dict[Optional[str], Tuple[str, Dict[str, Any]]] = {}
        self._subscribers: Set["Subscriber"] = set()
        self._stopped = False
        threading.Thread(target=self._flush_loop, name="event-broker",
                         daemon=True).start()

    # ------------------------------------------------------------------ #
    # Publication
    # ------------------------------------------------------------------ #
    def _next_seq_locked(self) -> int:
        self._seq += 1
        return self._seq

    def publish(self, etype: str, run_id: Optional[str],
                payload: Dict[str, Any]) -> None:
        """Publish immediately (ordering point for non-coalesced events).

        Pending coalesced progress is drained first, *under the same lock and
        before the sequence is assigned*, so a status event published a moment
        after progress can never get a lower sequence than that progress.
        """
        with self._cond:
            self._drain_pending_locked()
            self._dispatch_locked(
                self._next_seq_locked(), etype, run_id, payload)
            self._cond.notify_all()

    def publish_coalesced(self, etype: str, run_id: Optional[str],
                          payload: Dict[str, Any]) -> None:
        """Publish a high-frequency frame; newer frames replace older ones.

        The frame is held until the next flusher tick (or until an
        immediate event for the same ordering domain arrives), which caps the
        wire rate at ~10 fps regardless of how fast the simulation runs.
        """
        with self._cond:
            self._pending[run_id] = (etype, payload)
            self._cond.notify_all()

    def _drain_pending_locked(self) -> None:
        """Assign sequences to and dispatch every pending coalesced frame."""
        if not self._pending:
            return
        # Deterministic order across runs; the global sequence still defines
        # the true ordering observed by every subscriber.
        for key in sorted(self._pending, key=lambda k: ("" if k is None else k)):
            etype, payload = self._pending.pop(key)
            self._dispatch_locked(
                self._next_seq_locked(), etype, key, payload)

    def _dispatch_locked(self, seq: int, etype: str, run_id: Optional[str],
                         payload: Dict[str, Any]) -> None:
        self._ring.append((seq, etype, run_id, payload))
        for sub in list(self._subscribers):
            if not sub.matches(etype, run_id):
                continue
            try:
                sub.queue.put_nowait((seq, etype, run_id, payload))
            except queue.Full:
                # Slow consumer: mark it for a resync rather than blocking the
                # simulation thread or letting its view silently fall behind.
                sub.need_resync = True
                # Drop its backlog; the resync hello replaces it entirely.
                try:
                    while True:
                        sub.queue.get_nowait()
                except queue.Empty:
                    pass

    def _flush_loop(self) -> None:
        while True:
            with self._cond:
                if self._stopped:
                    return
                self._cond.wait(timeout=_FLUSH_INTERVAL)
                if self._stopped:
                    return
                self._drain_pending_locked()

    def current_seq(self) -> int:
        with self._lock:
            return self._seq

    # ------------------------------------------------------------------ #
    # Subscription
    # ------------------------------------------------------------------ #
    def subscribe(self, run_ids: Optional[Set[str]] = None,
                  global_scope: bool = False,
                  since: int = 0) -> "Subscriber":
        """Register a subscriber and replay events with seq > ``since``.

        ``global_scope`` subscribers (the overview / experiment pages) receive
        everything; run-scoped subscribers only receive their runs.  When the
        requested replay point has already fallen out of the ring the
        subscriber is flagged for resync — there is no way to reconstruct the
        gap from memory.
        """
        with self._cond:
            sub = Subscriber(run_ids or set(), global_scope)
            self._subscribers.add(sub)
            if since:  # any nonzero checkpoint (endpoint clamps negatives to 0)
                # Resync when the gap is unrecoverable from memory:
                #  * checkpoint newer than our head -> our sequence space was
                #    reset (server restarted while the client was offline);
                #  * checkpoint older than the ring's oldest retained event ->
                #    offline longer than the replay window.
                # A checkpoint inside the window replays only the missing tail.
                if since > self._seq or (self._ring and since < self._ring[0][0]):
                    sub.need_resync = True
                else:
                    try:
                        for seq, etype, rid, payload in self._ring:
                            if seq <= since:
                                continue
                            if sub.matches(etype, rid):
                                sub.queue.put_nowait(
                                    (seq, etype, rid, payload))
                    except queue.Full:
                        sub.need_resync = True
                        sub.queue = queue.Queue(maxsize=_QUEUE_MAX)
            self._cond.notify_all()
            return sub

    def unsubscribe(self, sub: "Subscriber") -> None:
        with self._lock:
            self._subscribers.discard(sub)


class Subscriber:
    """One SSE connection's filter, backlog queue and resync flag."""

    def __init__(self, run_ids: Set[str], global_scope: bool) -> None:
        self.run_ids = run_ids
        self.global_scope = global_scope
        self.queue: "queue.Queue[Tuple[int, str, Optional[str], Dict[str, Any]]]" = \
            queue.Queue(maxsize=_QUEUE_MAX)
        self.need_resync = False

    def matches(self, etype: str, run_id: Optional[str]) -> bool:
        if self.global_scope:
            return True
        if etype in RUN_SCOPED_TYPES:
            return run_id is not None and run_id in self.run_ids
        return False


# Process-wide singleton.
broker = EventBroker()


# --------------------------------------------------------------------------- #
# Convenience publishers — the single place run/experiment messages are shaped
# --------------------------------------------------------------------------- #
def emit_progress(run_id: str, step: int, total: int,
                  stats: Dict[str, Any]) -> None:
    frac = (float(step) / total) if total else None
    broker.publish_coalesced(PROGRESS, run_id, {
        "run_id": run_id, "step": int(step), "total_steps": int(total),
        "fraction": frac, "stats": stats,
    })


def emit_run_status(meta: Dict[str, Any]) -> None:
    broker.publish(RUN_STATUS, meta["id"], {
        "run_id": meta["id"], "name": meta.get("name"),
        "status": meta.get("status"),
        "current_step": meta.get("current_step", 0),
        "total_steps": meta.get("total_steps", 0),
    })


def emit_run_event(run_id: str, event: Dict[str, Any]) -> None:
    broker.publish(RUN_EVENT, run_id, {"run_id": run_id, "event": event})


def emit_run_created(meta: Dict[str, Any]) -> None:
    broker.publish(RUN_CREATED, meta["id"], {
        "run_id": meta["id"], "name": meta.get("name"),
        "status": meta.get("status"),
        "current_step": meta.get("current_step", 0),
    })


def emit_run_deleted(run_id: str) -> None:
    broker.publish(RUN_DELETED, run_id, {"run_id": run_id})


def emit_experiment_status(exp_id: str, status: str,
                           extra: Optional[Dict[str, Any]] = None) -> None:
    payload: Dict[str, Any] = {"experiment_id": exp_id, "status": status}
    if extra:
        payload.update(extra)
    broker.publish(EXPERIMENT_STATUS, None, payload)


# --------------------------------------------------------------------------- #
# SSE framing
# --------------------------------------------------------------------------- #
def _sse(event: str, data: Any, seq: Optional[int] = None) -> bytes:
    lines = [f"event: {event}"]
    if seq is not None:
        lines.append(f"id: {seq}")
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    # SSE frames are separated by a blank line; payload newlines must be split
    # into multiple "data:" lines, but compact JSON never contains raw newlines.
    lines.append(f"data: {body}")
    return ("\n".join(lines) + "\n\n").encode("utf-8")


def stream(sub: Subscriber):
    """Generator yielding SSE frames for a subscribed connection.

    Yields a ``hello`` frame first (``reason=resync`` when the client must
    re-fetch state, ``reason=ready`` for a clean start), then live frames and
    15-second heartbeat comments.  The Flask view unsubscribes when this
    generator is closed on client disconnect.
    """
    # Hint the browser's reconnect backoff so a blip recovers quickly.
    yield b"retry: 1000\n\n"
    hello_seq = broker.current_seq()
    reason = "resync" if sub.need_resync else "ready"
    yield _sse("hello", {"reason": reason}, seq=hello_seq)

    while True:
        try:
            item = sub.queue.get(timeout=_HEARTBEAT_INTERVAL)
        except queue.Empty:
            yield b": ping\n\n"
            continue

        if sub.need_resync:
            # Drain everything queued up to this point; the client replaces
            # its whole view with a fresh REST fetch keyed to hello_seq.
            try:
                while True:
                    sub.queue.get_nowait()
            except queue.Empty:
                pass
            sub.need_resync = False
            hello_seq = broker.current_seq()
            yield _sse("hello", {"reason": "resync"}, seq=hello_seq)
            continue

        seq, etype, _rid, payload = item
        yield _sse(etype, payload, seq=seq)
