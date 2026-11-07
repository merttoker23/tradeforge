"use strict";
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { candles, viewport, compatibleLevels } = require("../../src/tradeforge/dashboard/static/chart.js");

test("chart rejects malformed financial data instead of drawing fabricated candles", () => {
  assert.equal(candles([{time: 1, open: "1", high: "3", low: "0.5", close: "2", volume: "10"}]).length, 1);
  for (const row of [{time:1,open:0,high:1,low:0,close:1,volume:2}, {time:1,open:1,high:0.5,low:1,close:2,volume:3}, {time:1,open:"NaN",high:3,low:1,close:2,volume:4}]) assert.equal(candles([row]).length, 0);
  assert.equal(candles([]).length, 0);
});
test("zoom and pan stay inside bounded real series", () => {
  const data = Array.from({length: 300}, (_, i) => i);
  assert.equal(viewport(data, 80, 0).length, 80);
  assert.equal(viewport(data, 80, 0)[79], 299);
  assert.equal(viewport(data, 80, 10000)[0], 0);
  assert.equal(viewport([], 80, 0).length, 0);
});
test("Demo entry/exit/stop levels cannot be painted on public mainnet candles", () => {
  const levels = [{environment:"BINANCE_FUTURES_DEMO",symbol:"BTCUSDT",price:"100",kind:"entry"}];
  assert.equal(compatibleLevels(levels,"BINANCE_MAINNET_PUBLIC","BTCUSDT").length,0);
  assert.equal(compatibleLevels(levels,"BINANCE_FUTURES_DEMO","BTCUSDT").length,1);
  assert.equal(compatibleLevels(levels,"BINANCE_FUTURES_DEMO","ETHUSDT").length,0);
});

test("poll refresh preserves the user's chart zoom and pan",()=>{
  const { TerminalChart }=require("../../src/tradeforge/dashboard/static/chart.js");
  const instance=Object.create(TerminalChart.prototype);instance.draw=()=>{};
  const data={symbol:"BTCUSDT",timeframe:"1m",environment:"BINANCE_MAINNET_PUBLIC",bars:[]};
  instance.set(data);instance.offset=20;instance.count=40;instance.set(data);
  assert.equal(instance.offset,20);assert.equal(instance.count,40);
  instance.set({...data,symbol:"ETHUSDT"});assert.equal(instance.offset,0);
});
