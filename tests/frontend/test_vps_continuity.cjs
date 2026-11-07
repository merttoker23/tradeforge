"use strict";
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const js = readFileSync(resolve(__dirname, "../../src/tradeforge/dashboard/static/app.js"), "utf8");

test("background tabs never force a stale event", () => {
  assert.doesNotMatch(js, /visibilitychange[^\n]*stale\(/);
});
test("temporary transport errors retain the last verified account presentation", () => {
  const part = js.slice(js.indexOf("function stale("), js.indexOf("function expireSources("));
  assert.doesNotMatch(part, /snapshot\s*=\s*null|render\(\{status/);
  assert.match(js, /last verified|Son doğrulanmış|son doğrulanmış/i);
});
