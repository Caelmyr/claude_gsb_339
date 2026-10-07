/* Resumable Server-Sent-Events client with per-page subscription isolation.
 *
 * A page creates one EventStream for exactly the topics it currently needs.
 * Switching runs/pages closes that EventSource, so events cannot leak between
 * views.  EventSource itself reconnects after transient network failures using
 * Last-Event-ID; when the server cannot replay the gap, onResync performs one
 * authoritative REST reload while live events are buffered and applied after it.
 */

class EventStream {
  constructor(topics, handlers = {}) {
    this.topics = topics;
    this.handlers = handlers;
    this.source = null;
    this.closed = false;
    this.syncing = false;
    this.retryTimer = null;
    this.retryDelay = 1000;
    this.buffer = [];
    this._start();
  }

  _start() {
    const url = "/api/events?" + this.topics.map((t) => `topic=${encodeURIComponent(t)}`).join("&");
    this.source = new EventSource(url);
    this.source.addEventListener("open", () => { this.retryDelay = 1000; });
    this.source.addEventListener("hello", (e) => this._hello(e));
    for (const type of ["run.status", "run.progress", "run.event",
                        "run.created", "run.deleted", "experiment.status"]) {
      this.source.addEventListener(type, (e) => this._message(type, e));
    }
    this.source.onerror = () => {
      // EventSource reconnects automatically. Do not clear Last-Event-ID.
      if (this.handlers.connection) this.handlers.connection("reconnecting");
    };
  }

  async _hello(event) {
    let msg = {};
    try { msg = JSON.parse(event.data); } catch (e) { msg = {}; }
    if (msg.resync) {
      this.syncing = true;
      if (this.handlers.connection) this.handlers.connection("resync");
      await this._resyncLoop();
    } else if (this.handlers.connection) {
      this.handlers.connection("live");
    }
  }

  async _resyncLoop() {
    if (!this.handlers.resync) {
      this._finishSync();
      return;
    }
    try {
      await this.handlers.resync();
      this.retryDelay = 1000;
      this._finishSync();
    } catch (e) {
      if (this.closed) return;
      if (this.handlers.error) this.handlers.error(e);
      clearTimeout(this.retryTimer);
      this.retryTimer = setTimeout(() => this._resyncLoop(), this.retryDelay);
      this.retryDelay = Math.min(this.retryDelay * 2, 10000);
    }
  }

  _finishSync() {
    const events = this.buffer;
    this.buffer = [];
    this.syncing = false;
    events.forEach(({ type, data }) => this._dispatch(type, data));
    if (this.handlers.connection) this.handlers.connection("live");
  }

  _message(type, event) {
    let data;
    try { data = JSON.parse(event.data); }
    catch (e) { return; }
    if (this.syncing) this.buffer.push({ type, data });
    else this._dispatch(type, data);
  }

  _dispatch(type, data) {
    const fn = this.handlers[type] || this.handlers.message;
    if (fn) fn(data, type);
  }

  close() {
    this.closed = true;
    clearTimeout(this.retryTimer);
    if (this.source) this.source.close();
  }
}

function subscribeRun(runId, kinds, handlers) {
  const topics = kinds.map((kind) => `run:${runId}:${kind}`);
  return new EventStream(topics, handlers);
}
