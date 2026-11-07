"use strict";

const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { test } = require("node:test");
const { runInNewContext } = require("node:vm");

const source = readFileSync(resolve(__dirname, "../../src/tradeforge/dashboard/static/app.js"), "utf8");
const start = source.indexOf("function expireSources(data) {");
const end = source.indexOf("function render(data) {", start);
assert.ok(start >= 0 && end > start);
const now = Date.parse("2026-10-22T00:00:00Z");
const age = runInNewContext(source.slice(start, end) + "\nexpireSources", {
  Date: class extends Date { static now() { return now; } }
});
const timestamp = seconds => new Date(now + seconds * 1000).toISOString();

function snapshot(overrides = {}, status = "OK") {
  return {
    status: "DEGRADED",
    service_health: { market_gateway: "OK" },
    data_readiness: { streams: status, universe: "OK" },
    sections: {
      universe: { status: "OK", data: [{ eligible: 2, version_hash: "u1", stream_coverage: null, freshness: "OK", fresh_until: timestamp(60) }] },
      streams: { status, data: [{ eligible: 2, covered: 2, measured: true, clock_safe: true, universe_version: "u1", coverage_deadlines: [timestamp(2), timestamp(10)], fresh_until: timestamp(15), freshness: status, ...overrides }] },
      workers: { status: "OK", data: [{ service: "market_gateway", freshness: "OK", fresh_until: timestamp(15) }] }
    }
  };
}

for (const [name, overrides, state] of [
  ["unsafe clock", { clock_safe: false }, "UNKNOWN"],
  ["unmeasured stream", { measured: false }, "UNKNOWN"],
  ["unknown source", {}, "UNKNOWN"],
  ["stale source", {}, "STALE"],
  ["mismatched universe", { universe_version: "different" }, "OK"]
]) {
  test(name + " cannot acquire numeric coverage in the browser", () => {
    const data = snapshot(overrides, state);
    age(data);
    assert.equal(data.sections.universe.data[0].stream_coverage, null);
  });
}

test("current measured coverage can be shown", () => {
  const data = snapshot();
  age(data);
  assert.equal(data.sections.universe.data[0].stream_coverage, 1);
});

test("already degraded coverage redraws as another symbol expires", () => {
  const data = snapshot({ coverage_deadlines: [timestamp(-1), timestamp(10)] }, "DEGRADED");
  assert.equal(age(data), true);
  assert.equal(data.sections.universe.data[0].stream_coverage, 0.5);
  assert.equal(data.service_health.market_gateway, "OK");
});

test("an expired measurement clears coverage and ages summaries", () => {
  const data = snapshot({ fresh_until: timestamp(-1) });
  assert.equal(age(data), true);
  assert.equal(data.sections.universe.data[0].stream_coverage, null);
  assert.equal(data.data_readiness.streams, "STALE");
});

test("expired discovery cannot retain coverage", () => {
  const data = snapshot();
  data.sections.universe.data[0].fresh_until = timestamp(-1);
  age(data);
  assert.equal(data.sections.universe.data[0].stream_coverage, null);
  age(data);
  assert.equal(data.sections.universe.data[0].stream_coverage, null);
});

test("expired demo diagnostics cannot retain a successful connection summary", () => {
  const data = snapshot();
  data.demo_readiness = { status: "OK", orders_enabled: false, reason_codes: [] };
  data.service_health.execution_guardian = "OK";
  data.sections.demo = { status: "OK", data: [{ guardian: "RUNNING", freshness: "OK", fresh_until: timestamp(-1) }] };
  age(data);
  assert.equal(data.demo_readiness.status, "STALE");
  assert.equal(data.demo_readiness.orders_enabled, false);
  assert.equal(data.demo_readiness.reason_codes.length, 1);
  assert.equal(data.demo_readiness.reason_codes[0], "DEMO_PREFLIGHT_STALE");
  assert.equal(data.service_health.execution_guardian, "NOT_READY");
  assert.equal(data.sections.demo.data[0].guardian, "UNKNOWN");
});

test("lost dashboard connection clears derived demo health before source expiry", () => {
  const data = snapshot();
  data.status = "STALE";
  data.demo_readiness = { status: "OK", orders_enabled: false, reason_codes: [] };
  data.service_health.execution_guardian = "UNKNOWN";
  data.sections.demo = { status: "STALE", data: [{ guardian: "RUNNING", freshness: "STALE", fresh_until: timestamp(20) }] };
  age(data);
  assert.equal(data.demo_readiness.status, "STALE");
  assert.equal(data.service_health.execution_guardian, "NOT_READY");
  assert.equal(data.sections.demo.data[0].guardian, "UNKNOWN");
});
