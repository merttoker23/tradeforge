"use strict";

const { test } = require("node:test");
const assert = require("node:assert/strict");
const { readFileSync, existsSync } = require("node:fs");
const { resolve } = require("node:path");
const { runInNewContext } = require("node:vm");
const path = resolve(__dirname, "../../src/tradeforge/dashboard/static/login.js");

function login(request = async () => ({ ok: false, status: 401 })) {
  const elements = new Map();
  const listeners = {};
  const calls = [];
  const destinations = [];
  const timers = new Map();
  function get(id) {
    if (!elements.has(id)) elements.set(id, {
      value: "", type: "password", textContent: "", hidden: true, disabled: false,
      validity: { valid: true }, attributes: {}, listeners: {},
      setAttribute(name, value) { this.attributes[name] = value; },
      removeAttribute(name) { delete this.attributes[name]; },
      getAttribute(name) { return this.attributes[name]; },
      addEventListener(name, handler) { this.listeners[name] = handler; },
      focus() { this.focused = true; },
    });
    return elements.get(id);
  }
  const form = get("login-form");
  get("login-submit").disabled = true;
  form.action = "https://trade.example.com/login";
  form.querySelector = () => get("spinner");
  const window = {
    location: { origin: "https://trade.example.com", replace: path => destinations.push(path) },
    addEventListener: (name, handler) => { listeners[name] = handler; },
  };
  runInNewContext(existsSync(path) ? readFileSync(path, "utf8") : "", {
    document: { getElementById: get }, window, URL, AbortController,
    FormData: class extends Map { constructor() { super([["token", get("session-key").value]]); } },
    setTimeout: callback => { const id = timers.size + 1; timers.set(id, callback); return id; },
    clearTimeout: id => timers.delete(id),
    fetch: async (path, options) => { calls.push({ path, options }); return request(path, options); },
  });
  return {
    get, calls, destinations, listeners, timers,
    submit: () => {
      assert.ok(form.listeners.submit, "the login form should handle submission");
      return form.listeners.submit({ preventDefault() {} });
    },
  };
}

test("the key starts hidden and visibility can be toggled accessibly", () => {
  const a = login();
  const toggle = a.get("toggle-key"), key = a.get("session-key");
  assert.equal(toggle.hidden, false);
  assert.equal(a.get("login-submit").disabled, false);
  toggle.listeners.click();
  assert.equal(key.type, "text");
  assert.equal(toggle.getAttribute("aria-pressed"), "true");
  assert.match(toggle.getAttribute("aria-label"), /gizle/);
  toggle.listeners.click();
  assert.equal(key.type, "password");
  assert.equal(toggle.getAttribute("aria-pressed"), "false");
  toggle.listeners.click();
  a.listeners.blur();
  assert.equal(key.type, "password");
});

test("empty or malformed input reports a field error without a request", async () => {
  for (const value of ["", "short", " ".repeat(64)]) {
    const a = login();
    a.get("session-key").value = value;
    a.get("session-key").validity.valid = false;
    await a.submit();
    assert.equal(a.calls.length, 0);
    assert.equal(a.get("session-key").getAttribute("aria-invalid"), "true");
    assert.equal(a.get("session-key").focused, true);
    assert.ok(a.get("login-error").textContent);
  }
});

test("a pending POST preserves the key, hides it, and prevents duplicate requests", async () => {
  let finish;
  const a = login(() => new Promise(resolve => { finish = resolve; }));
  const token = "x".repeat(64);
  a.get("session-key").value = token;
  a.get("toggle-key").listeners.click();
  const pending = a.submit();
  await a.submit();
  assert.equal(a.calls.length, 1);
  assert.equal(a.calls[0].path, "/login");
  assert.equal(a.calls[0].options.method, "POST");
  assert.equal(a.calls[0].options.credentials, "same-origin");
  assert.equal(a.calls[0].options.body.get("token"), token);
  assert.equal(a.get("session-key").type, "password");
  assert.equal(a.get("login-form").getAttribute("aria-busy"), "true");
  assert.equal(a.get("login-submit").disabled, true);
  assert.ok(a.get("login-status").textContent);
  finish({ ok: true, status: 200, redirected: true, url: "https://trade.example.com/" });
  await pending;
  assert.deepEqual(a.destinations, ["/"]);
  assert.equal(a.get("session-key").value, "");
  assert.equal(a.timers.size, 0);
});

test("rejected credentials, rate limits and server errors allow another attempt", async () => {
  for (const [status, message] of [[401, /doğrulanamadı/], [403, /yenileyip/], [429, /Çok fazla/], [503, /geçici/]]) {
    const a = login(async () => ({ ok: false, status }));
    a.get("session-key").value = "x".repeat(64);
    await a.submit();
    assert.match(a.get("login-error").textContent, message);
    assert.equal(a.get("login-submit").disabled, false);
    assert.equal(a.get("session-key").disabled, false);
    assert.equal(a.get("login-form").getAttribute("aria-busy"), "false");
    assert.equal(a.destinations.length, 0);
    assert.equal(a.timers.size, 0);
    a.get("session-key").listeners.input();
    assert.equal(a.get("login-error").textContent, "");
    assert.equal(a.get("session-key").getAttribute("aria-invalid"), undefined);
  }
});

test("network failures and timeouts recover without retrying the key automatically", async () => {
  const offline = login(async () => { throw new Error("offline"); });
  await offline.submit();
  assert.match(offline.get("login-error").textContent, /Bağlantı/);
  assert.equal(offline.get("login-submit").disabled, false);
  assert.equal(offline.calls.length, 1);
  const timed = login((path, options) => new Promise((resolve, reject) => {
    options.signal.addEventListener("abort", () => reject(new Error("timeout")));
  }));
  const pending = timed.submit();
  [...timed.timers.values()][0]();
  await pending;
  assert.equal(timed.get("login-submit").disabled, false);
  assert.match(timed.get("login-error").textContent, /Bağlantı/);
  assert.equal(timed.calls.length, 1);
});

test("an unexpected response never navigates to an untrusted destination", async () => {
  for (const url of ["https://evil.invalid/", "https://trade.example.com/login"]) {
    const a = login(async () => ({ ok: true, status: 200, redirected: true, url }));
    await a.submit();
    assert.deepEqual(a.destinations, []);
    assert.ok(a.get("login-error").textContent);
    assert.equal(a.get("login-submit").disabled, false);
  }
});

test("a restored history page clears the secret and resets loading controls", async () => {
  const a = login();
  a.get("session-key").value = "x".repeat(64);
  a.get("login-submit").disabled = true;
  a.listeners.pageshow({ persisted: true });
  assert.equal(a.get("session-key").value, "");
  assert.equal(a.get("session-key").type, "password");
  assert.equal(a.get("login-submit").disabled, false);
});

test("an old response cannot reset a new login attempt after history restoration", async () => {
  const responses = [];
  const a = login(() => new Promise(resolve => responses.push(resolve)));
  a.get("session-key").value = "x".repeat(64);
  const old = a.submit();
  a.listeners.pageshow({ persisted: true });
  assert.equal(a.calls[0].options.signal.aborted, true);
  a.get("session-key").value = "x".repeat(64);
  const current = a.submit();
  responses[0]({ ok: false, status: 401 });
  await old;
  assert.equal(a.get("login-submit").disabled, true);
  assert.equal(a.get("login-error").textContent, "");
  await a.submit();
  assert.equal(a.calls.length, 2);
  responses[1]({ ok: false, status: 401 });
  await current;
  assert.equal(a.get("login-submit").disabled, false);
  assert.ok(a.get("login-error").textContent);
});
