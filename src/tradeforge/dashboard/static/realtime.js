"use strict";
// One authenticated transport for every dashboard projection.
window.DashboardSocket = class DashboardSocket {
  constructor(handlers) {
    this.handlers = handlers;
    this.socket = null;
    this.ready = false;
    this.sequence = 0;
    this.nextId = 0;
    this.pending = new Map();
    this.subscriptions = new Map();
    this.retry = null;
    this.attempt = 0;
    this.lastMessage = 0;
    this.handshake = null;
    setInterval(() => {
      if (this.ready && Date.now() - this.lastMessage > 45000) this.fail("HEARTBEAT_TIMEOUT");
    }, 5000);
  }
  connect() {
    if (this.socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(this.socket.readyState)) return;
    if (this.retry !== null) {clearTimeout(this.retry); this.retry = null;}
    const scheme = location.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(scheme + "//" + location.host + "/api/realtime");
    this.socket = socket;
    this.ready = false;
    this.sequence = 0;
    this.handshake = setTimeout(() => {if (this.socket === socket && !this.ready) this.fail("HANDSHAKE_TIMEOUT");}, 10000);
    socket.addEventListener("message", event => {
      if (this.socket !== socket) return;
      try {this.accept(JSON.parse(event.data));} catch {this.fail("INVALID_FRAME");}
    });
    socket.addEventListener("close", () => this.lost(socket));
    socket.addEventListener("error", () => {if (this.socket === socket) this.fail("CONNECTION_LOST");});
  }
  fail(reason) {
    const socket = this.socket;
    this.lost(socket);
    socket?.close(4000, reason);
  }
  lost(socket) {
    if (this.socket !== socket || !socket) return;
    this.socket = null;
    this.ready = false;
    clearTimeout(this.handshake);
    this.subscriptions.clear();
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.cleanup();
      pending.reject(new Error("CONNECTION_LOST"));
    }
    this.pending.clear();
    this.handlers.onDisconnect?.();
    if (this.retry === null) {
      const delay = Math.min(30000, 1000 * 2 ** Math.min(this.attempt++, 5));
      this.retry = setTimeout(() => {this.retry = null; this.connect();}, delay + Math.round(Math.random() * delay / 4));
    }
  }
  accept(frame) {
    if (!frame || !Number.isSafeInteger(frame.seq) || frame.seq !== this.sequence + 1) return this.fail("SEQUENCE_GAP");
    this.sequence = frame.seq;
    this.lastMessage = Date.now();
    if (frame.type === "hello") {
      if (this.ready || frame.protocol !== 2 || !Array.isArray(frame.session?.scopes)) return this.fail("PROTOCOL_MISMATCH");
      this.ready = true;
      this.attempt = 0;
      clearTimeout(this.handshake);
      this.handlers.onSession?.(frame.session);
      return;
    }
    if (!this.ready) return this.fail("HELLO_REQUIRED");
    if (frame.type === "heartbeat") return;
    if (frame.type === "snapshot") {
      if (!frame.data || typeof frame.data !== "object") return this.fail("INVALID_SNAPSHOT");
      this.handlers.onSnapshot?.(frame.data);
    } else if (frame.type === "quotes") {
      this.handlers.onQuotes?.(frame);
    } else if (frame.type === "response") {
      const pending = this.pending.get(frame.id);
      if (!pending) return;
      this.pending.delete(frame.id);
      clearTimeout(pending.timer);
      pending.cleanup();
      if (frame.ok) pending.resolve(frame.data);
      else pending.reject(new Error(frame.error || "REQUEST_REJECTED"));
    } else if (frame.type === "symbols" || frame.type === "chart") {
      const subscription = this.subscriptions.get(frame.type);
      if (subscription?.id === frame.id) this.handlers.onUpdate?.({operation:frame.type,params:subscription.params,data:frame.data,error:!frame.ok});
    } else this.fail("INVALID_EVENT");
  }
  request(path, {timeout=12000, signal}={}) {
    const query = new URL(path, "https://" + location.host);
    const op = {"/api/snapshot":"snapshot", "/api/market/symbols":"symbols", "/api/market/chart":"chart", "/api/market/quotes":"quotes"}[query.pathname];
    if (!op) return Promise.reject(new Error("REQUEST_INVALID"));
    const params = op === "symbols" ? {search:query.searchParams.get("search") || "",offset:Number(query.searchParams.get("offset") || 0)} :
      op === "chart" ? {symbol:query.searchParams.get("symbol"),timeframe:query.searchParams.get("timeframe")} :
      op === "quotes" ? {symbols:(query.searchParams.get("symbols") || "").split(",").filter(Boolean)} : {};
    return this.send(op, params, timeout, signal);
  }
  send(op, params, timeout, signal) {
    if (!this.ready || this.socket?.readyState !== WebSocket.OPEN) return Promise.reject(new Error("CONNECTION_LOST"));
    if (signal?.aborted || this.pending.size >= 16) return Promise.reject(new Error("REQUEST_SUPERSEDED"));
    const id = ++this.nextId;
    if (op === "symbols" || op === "chart") this.subscriptions.set(op, {id,params});
    return new Promise((resolve, reject) => {
      const abort = () => {
        const pending = this.pending.get(id);
        if (!pending) return;
        this.pending.delete(id);
        clearTimeout(pending.timer);
        pending.cleanup();
        reject(new Error("REQUEST_SUPERSEDED"));
      };
      const timer = setTimeout(() => {
        if (!this.pending.has(id)) return;
        this.pending.delete(id);
        signal?.removeEventListener("abort", abort);
        reject(new Error("REQUEST_TIMEOUT"));
      }, timeout);
      const cleanup = () => signal?.removeEventListener("abort", abort);
      this.pending.set(id, {op,resolve,reject,timer,cleanup});
      signal?.addEventListener("abort", abort, {once:true});
      try {this.socket.send(JSON.stringify({type:"request",id,op,params}));}
      catch {this.fail("SEND_FAILED");}
    });
  }
};
