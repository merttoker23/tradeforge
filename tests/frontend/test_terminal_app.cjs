"use strict";
const assert=require("node:assert/strict");
const {test}=require("node:test");
const {readFileSync}=require("node:fs");
const {resolve}=require("node:path");
const {runInNewContext}=require("node:vm");
const source=readFileSync(resolve(__dirname,"../../src/tradeforge/dashboard/static/app.js"),"utf8");

class Element {
  constructor(){this.textContent="";this.value="";this.children=[];this.events={};this.dataset={};this.disabled=false;this.classList={toggle(){}};}
  replaceChildren(...nodes){this.children=nodes;this.textContent="";}
  append(...nodes){this.children.push(...nodes);}
  setAttribute(name,value){this[name]=value;}
  addEventListener(name,handler){this.events[name]=handler;}
  reset(){this.value="";}
  getAttribute(name){return this[name];}
  remove(){this.removed=true;}
}
async function app({holdSnapshot=false,proof=true,offline=false,busySemantics=false,scopes=["read","setup","shutdown"]}={}){
  let socketAdapter;const elements=new Map(),calls=[],timers=[],intervals=[],chartUpdates=[];let inFlight=0,maxInFlight=0,chartOffline=false,fetchGate=null,chartBody={symbol:"BTCUSDT",timeframe:"15m",bars:[]};let releaseSnapshot;let configured=true;let supervisor={state:"RUNNING",as_of:Date.now()/1000,services:{quant_core:{alive:true}}};const gate=new Promise(resolve=>{releaseSnapshot=resolve;});
  const get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
  let price="100",members=["BTCUSDT"],dbBusy=false,busyRejections=0,maxDbInFlight=0,dbInFlight=0;
  const data={status:"DEGRADED",as_of:new Date().toISOString(),sections:{demo:{status:"OK",data:[{connection:"OK",account:"OK",balance:"OK",positions:"OK",clock:"OK",equity:"1000",available_balance:"900",position_count:0,as_of:new Date().toISOString(),freshness:"OK",fresh_until:new Date(Date.now()+30000).toISOString(),terminal:{daily_pnl:"2",total_pnl:null,total_risk:null,positions:[]}}]},signals:{status:"UNKNOWN",data:[]}},service_health:{},data_readiness:{}};
  get("session-proof").content="csrf";
  const context={history:{replaceState(){}},document:{getElementById:id=>id==="session-proof"&&!proof?null:get(id),createElement:()=>new Element(),querySelectorAll:()=>[],addEventListener(){}},TerminalChart:class{set(data){chartUpdates.push(data);} draw(){}},Date,performance:{now:()=>1},AbortSignal,AbortController,crypto:{randomUUID:()=>"id"},setTimeout:callback=>{timers.push(callback);return timers.length;},clearTimeout(){},setInterval:callback=>{intervals.push(callback);return intervals.length;},confirm:()=>true,console,
    transportRead:async(path,options)=>{inFlight++;maxInFlight=Math.max(maxInFlight,inFlight);
      const isDb=path==="/api/snapshot"||path.startsWith("/api/market/symbols")||path.startsWith("/api/market/chart");
      let admitted=false;
      try{
      // Mirror api.py: uncached DB reads are rejected immediately while busy.
      if(isDb&&busySemantics&&dbBusy){busyRejections++;calls.push({path,options});return {ok:false,status:429,json:async()=>({error:"REQUEST_REJECTED"})};}
      if(isDb){admitted=true;dbBusy=true;dbInFlight++;maxDbInFlight=Math.max(maxDbInFlight,dbInFlight);}
const capturedChartBody=chartBody;await new Promise(setImmediate);if(fetchGate)await fetchGate(path);calls.push({path,options});if(offline)throw new Error("OFFLINE");let body;
      if(path==="/api/session")body={scopes,csrf:"csrf"};
      else if(path==="/api/setup/demo")body={configured,storage:"PRIVATE_FILE",supervisor};
      else if(path==="/api/snapshot"){if(holdSnapshot)await gate;body=data;}
      else if(path.startsWith("/api/market/symbols"))body={items:members.map(symbol=>({symbol,price,volume:"20",source_at:new Date().toISOString()})),has_more:false};
      else if(path.startsWith("/api/market/quotes"))body={transport:"OK",items:decodeURIComponent(path.split("symbols=")[1]).split(",").map(symbol=>({symbol,price,volume:"20",freshness:"OK",source_at:new Date().toISOString()}))};
      else if(path.startsWith("/api/market/chart")){if(chartOffline)throw new Error("TEMPORARY_CHART_FAILURE");body=capturedChartBody;}
      else body={};
      return {ok:true,status:200,json:async()=>body};
    }finally{inFlight--;if(admitted){dbInFlight--;dbBusy=dbInFlight>0;}}}};
  // Render/interaction tests inject a socket adapter. The real two-script
  // transport test separately forbids every fetch call.
  context.DashboardSocket=class {
    constructor(handlers){this.handlers=handlers;this.ready=false;socketAdapter=this;}
    connect(){if(offline){this.handlers.onDisconnect();timers.push(()=>this.connect());return;}this.ready=true;this.handlers.onSession({scopes});(async()=>{if(holdSnapshot)await gate;this.handlers.onSnapshot(data);})();}
    async request(path,options={}){return (await context.transportRead(path,options)).json();}
  };
  runInNewContext(source,context);for(let i=0;i<15;i++)await new Promise(setImmediate);
  return {get,calls,context,data,timers,intervals,releaseSnapshot,chartUpdates,pushSnapshot:()=>socketAdapter.handlers.onSnapshot(data),get maxInFlight(){return maxInFlight;},get busyRejections(){return busyRejections;},get maxDbInFlight(){return maxDbInFlight;},setFetchGate:gate=>{fetchGate=gate;},setChartBody:b=>{chartBody=b;},setChartOffline:v=>{chartOffline=v;},setSupervisor:s=>{supervisor=s;},setConfigured:c=>{configured=c;},setPrice:p=>{price=p;},setMembers:m=>{members=m;},setOffline:value=>{offline=value;}};
}
test("normal startup uses the read-only observer without bearer authority",async()=>{
  const a=await app();assert.ok(a.calls[0].path.startsWith("/api/market/symbols"));assert.equal(a.calls[0].options.headers?.Authorization,undefined);assert.equal(a.get("equity").textContent,"1,000.00");assert.equal(a.get("decision").textContent,"NO_TRADE");
});

test("overview reports server testnet authority separately from the read-only session",async()=>{
  const a=await app();
  a.data.execution_readiness={ready:true,entries_enabled:true,trial_enabled:true,reason:"READY",blockers:[]};
  a.data.analysis_readiness={ready:true,reason:"WAITING_CLOSED_BAR"};
  a.context.render(a.data);
  assert.equal(a.get("runtime-mode").textContent,"TESTNET");
  assert.equal(a.get("execution-status").textContent,"Emir koşulları hazır");
  a.context.stale("Disconnected");
  assert.notEqual(a.get("execution-status").textContent,"Emir koşulları hazır");
});

test("a watching setup explains its stage rather than claiming all entries are disabled",async()=>{
  const a=await app();
  a.data.sections.signals={status:"OK",data:[{symbol:"BTCUSDT",state:"WATCHING",side:"LONG",alpha_family:"MOMENTUM",freshness:"OK",reason_codes:["PULLBACK_CONFIRMED"]}]};
  a.context.render(a.data);
  assert.equal(a.get("decision").textContent,"İZLENİYOR");
  assert.doesNotMatch(a.get("decision-explanation").textContent,/Yeni girişler kapalı/);
});

test("a read-only dashboard may reload without a tab-specific bootstrap proof",async()=>{
  const a=await app({proof:false});assert.ok(a.calls[0].path.startsWith("/api/market/symbols"));
});
test("a refreshed selected quote must replace the displayed old price",async()=>{
  const a=await app();assert.equal(a.get("selected-price").textContent,"100.00");a.setPrice("105");await a.context.loadSymbols();assert.equal(a.get("selected-price").textContent,"105.00");
});
test("expired Demo data retains historical values but marks them as old",async()=>{
  const a=await app();a.data.sections.demo.data[0].connection="OK";a.data.sections.demo.data[0].account="OK";
  a.data.sections.demo.data[0].fresh_until=new Date(Date.now()-1).toISOString();a.context.render(a.data);
  assert.equal(a.get("equity").textContent,"1,000.00");assert.match(a.get("account-note").textContent,/Son doğrulanmış/);
});
test("read-only browsing never requests write or retired setup routes",async()=>{
  const a=await app();
  assert.ok(a.calls.length>2);
  assert.ok(a.calls.every(c=>!c.options.method));
  assert.ok(a.calls.every(c=>!c.path.startsWith("/api/setup/")));
});

test("transport loss retains historical values but revokes current decisions",async()=>{
  const a=await app();a.data.sections.signals={status:"OK",data:[{symbol:"BTCUSDT",side:"LONG",state:"APPROVED",freshness:"OK"}]};
  a.context.render(a.data);assert.equal(a.get("decision").textContent,"LONG");
  a.context.stale("Disconnected");
  assert.equal(a.get("equity").textContent,"1,000.00");
  assert.equal(a.get("decision").textContent,"NO_TRADE");
  assert.match(a.get("account-note").textContent,/Son doğrulanmış/);
});

test("an unvalidated testnet trial is identified independently of statistical approval",async()=>{
  const a=await app();
  a.data.sections.signals={status:"OK",data:[{symbol:"BTCUSDT",side:"LONG",state:"APPROVED",freshness:"OK",execution_authority:"TESTNET_TRIAL",proof_state:"UNPROVEN"}]};
  a.context.render(a.data);
  assert.match(a.get("decision-explanation").textContent,/Testnet denemesi/);
  assert.match(a.get("decision-explanation").textContent,/UNPROVEN/);
});

test("an unavailable server during initial load recovers without reloading the tab",async()=>{
  const a=await app({offline:true});assert.ok(a.timers.length>0);
  a.setOffline(false);await a.timers.at(-1)();
  assert.equal(a.get("equity").textContent,"1,000.00");
});

test("transport and chart failures clear selected feature metrics",async()=>{
  const a=await app();
  a.get("selected-volatility").textContent="0.2";a.get("selected-strength").textContent="1.1";
  a.context.stale("Disconnected");
  assert.equal(a.get("selected-volatility").textContent,"—");
  assert.equal(a.get("selected-strength").textContent,"—");
  a.get("selected-volatility").textContent="0.3";a.setOffline(true);
  await a.context.loadChart();assert.equal(a.get("selected-volatility").textContent,"—");
});


test("independent read lanes stay bounded while allowing quotes past slower reads",async()=>{
  const a=await app();
  await Promise.all([a.context.api("/api/snapshot",()=>true,"snapshot"),a.context.loadChart(),a.context.loadSymbols()]);
  assert.equal(a.maxDbInFlight,1);
});

test("temporary chart refresh failure retains candles and clears current metrics",async()=>{
  const a=await app();
  a.setChartBody({symbol:"BTCUSDT",timeframe:"15m",bars:[{time:1792872000,open:100,high:101,low:99,close:100,volume:1}]});
  await a.context.loadChart();const previous=a.chartUpdates.at(-1);
  a.get("selected-volatility").textContent="0.2";
  a.setChartOffline(true);await a.context.loadChart();
  assert.equal(a.chartUpdates.at(-1),previous);
  assert.equal(a.get("selected-volatility").textContent,"—");
  assert.match(a.get("chart-status").textContent,/son doğrulanmış/i);
});

test("switching symbols clears old candles even when the new request fails",async()=>{
  const a=await app();
  a.setChartBody({symbol:"BTCUSDT",timeframe:"15m",bars:[{time:1792872000,open:100,high:101,low:99,close:100,volume:1}]});
  await a.context.loadChart();a.setChartOffline(true);
  await a.context.selectSymbol("ETHUSDT");
  assert.equal(a.get("selected-symbol").textContent,"ETHUSDT");
  assert.equal(a.chartUpdates.at(-1).bars.length,0);
  assert.equal(a.get("chart-time").textContent,"—");
});


test("superseded queued symbol reads are dropped without replacing the symbol list",async()=>{
  const a=await app();let release;const gate=new Promise(resolve=>{release=resolve;});
  a.setFetchGate(path=>path.startsWith("/api/market/chart")?gate:undefined);
  const held=a.context.loadChart();await new Promise(setImmediate);
  a.get("search").value="OLD";const old=a.context.loadSymbols();
  a.get("search").value="NEW";const latest=a.context.loadSymbols();
  release();await Promise.all([held,old,latest]);
  assert.ok(!a.calls.some(c=>c.path.includes("search=OLD")));
  assert.ok(a.calls.some(c=>c.path.includes("search=NEW")));
  assert.ok(a.maxInFlight<=4);
});

test("an older pending chart response cannot replace a newly selected symbol",async()=>{
  const a=await app();let releaseOld,releaseNew;
  const oldGate=new Promise(resolve=>{releaseOld=resolve;}),newGate=new Promise(resolve=>{releaseNew=resolve;});
  a.setFetchGate(path=>path.includes("chart?symbol=BTCUSDT")?oldGate:path.includes("chart?symbol=ETHUSDT")?newGate:undefined);
  const old=a.context.loadChart();await new Promise(setImmediate);
  a.setChartBody({symbol:"ETHUSDT",timeframe:"15m",bars:[{time:1792872000,open:100,high:101,low:99,close:100,volume:1}]});
  const selected=a.context.selectSymbol("ETHUSDT"),cleared=a.chartUpdates.length;
  releaseOld();for(let i=0;i<5;i++)await new Promise(setImmediate);
  assert.equal(a.chartUpdates.length,cleared,"old response changed the new symbol's cleared chart");
  assert.equal(a.chartUpdates.at(-1).bars.length,0);
  releaseNew();await Promise.all([old,selected]);
  assert.equal(a.get("selected-symbol").textContent,"ETHUSDT");
  assert.equal(a.chartUpdates.at(-1).symbol,"ETHUSDT");
  assert.ok(a.maxInFlight<=4);
});

test("a slow chart cannot block fresh selected and off-page favorite quotes",async()=>{
  const a=await app();let release;const gate=new Promise(resolve=>{release=resolve;});
  a.setFetchGate(path=>path.startsWith("/api/market/chart")?gate:undefined);
  const held=a.context.loadChart();await new Promise(setImmediate);
  a.setPrice("109");
  try {
    assert.equal(typeof a.context.loadQuotes,"function");
    await a.context.loadQuotes();
    assert.equal(a.get("selected-price").textContent,"109.00");
    assert.ok(a.calls.some(c=>c.path.startsWith("/api/market/quotes?symbols=BTCUSDT")));
  } finally {release();await held;}
});

test("a search input immediately fences a previous response before its debounce",async()=>{
  const a=await app();let release;const gate=new Promise(resolve=>{release=resolve;});
  a.setFetchGate(path=>path.includes("search=OLD")?gate:undefined);
  a.get("search").value="OLD";const old=a.context.loadSymbols();await new Promise(setImmediate);
  a.get("search").value="NEW";a.get("search").events.input();release();await old;
  assert.ok(!a.get("symbols").children.some(row=>row.children?.some(cell=>cell.children?.some(label=>label.textContent==="BTCUSDT"))));
});

test("one quote request covers selected, current page and off-page favorites",async()=>{
  const a=await app();
  a.setMembers(["ETHUSDT"]);await a.context.loadSymbols();
  a.get("symbols").children[0].children[0].events.click();
  a.setMembers(["SOLUSDT"]);await a.context.loadSymbols();
  a.setPrice("112");await a.context.loadQuotes();
  const latest=a.calls.filter(c=>c.path.startsWith("/api/market/quotes")).at(-1);
  const members=decodeURIComponent(latest.path.split("symbols=")[1]).split(",");
  assert.deepEqual(members,["BTCUSDT","ETHUSDT","SOLUSDT"]);
  assert.equal(a.get("selected-price").textContent,"112.00");
});

test("quote transport failure retains original last-confirmed value with explicit stale text",async()=>{
  const a=await app();const original=a.get("selected-price").title;
  a.setOffline(true);await a.context.loadQuotes();
  assert.equal(a.get("selected-price").textContent,"100.00 · eski");
  assert.ok(a.get("selected-price").title.includes(original.split(" · ")[0]));
});

test("quote refresh is single flight and a selection replaces its obsolete subscription",async()=>{
  const a=await app();let release;const gate=new Promise(resolve=>{release=resolve;});
  a.setFetchGate(path=>path.startsWith("/api/market/quotes")?gate:undefined);
  const count=a.calls.filter(c=>c.path.startsWith("/api/market/quotes")).length;
  const first=a.context.loadQuotes(),second=a.context.loadQuotes();
  assert.equal(first,second);
  await new Promise(setImmediate);const selection=a.context.selectSymbol("ETHUSDT");
  release();await Promise.all([first,selection]);
  const requests=a.calls.filter(c=>c.path.startsWith("/api/market/quotes"));
  for(let i=0;i<5;i++)await new Promise(setImmediate);
  const replacement=a.calls.filter(c=>c.path.startsWith("/api/market/quotes")).at(-1);
  assert.equal(requests.length,count+2);
  assert.equal(requests.at(-2).options.signal.aborted,true);
  assert.ok(replacement.path.includes("ETHUSDT"));
  assert.equal(a.get("selected-symbol").textContent,"ETHUSDT");
});

test("health presentation distinguishes worker health, data, analysis and execution readiness",async()=>{
  const a=await app();a.data.sections.workers={status:"OK",data:[{service:"quant_core",freshness:"OK",healthy:true,data_ready:true,analysis_ready:false,execution_ready:false,stage:"FEATURES_UNAVAILABLE",last_progress_at:new Date().toISOString(),error_count:0}]};
  a.context.render(a.data);
  const text=a.get("data-health").children.map(row=>row.children.map(cell=>cell.textContent).join(" ")).join(" ");
  assert.match(text,/quant_core/);assert.match(text,/Analiz.*Hazır değil/);assert.match(text,/Emir.*Hazır değil/);assert.match(text,/FEATURES_UNAVAILABLE/);
});

test("uncached snapshot, symbols and chart all refresh under the API's fail-immediate DB busy lock",async()=>{
  const a=await app({busySemantics:true});
  assert.equal(a.get("equity").textContent,"1,000.00","startup snapshot was rejected while symbols held DB admission");
  for(let close=101;close<=103;close++){
    a.setChartBody({symbol:"BTCUSDT",timeframe:"15m",bars:[{time:1792872000,open:100,high:104,low:99,close,volume:1}]});
    await Promise.all([a.context.api("/api/snapshot",()=>true,"snapshot"),Promise.allSettled([a.context.loadSymbols(),a.context.loadChart()])]);
    assert.equal(a.chartUpdates.at(-1).bars[0]?.close,close,"periodic chart was starved by another uncached read");
  }
  assert.equal(a.busyRejections,0);
  assert.equal(a.maxDbInFlight,1);
});

test("the DB wait queue coalesces superseded searches and serves snapshot plus chart fairly while quotes bypass",async()=>{
  const a=await app({busySemantics:true});let release;const gate=new Promise(resolve=>{release=resolve;});
  a.setFetchGate(path=>path.startsWith("/api/market/chart")?gate:undefined);
  const held=a.context.loadChart();await new Promise(setImmediate);
  const pending=[];
  for(let i=0;i<100;i++){a.get("search").value="S"+i;pending.push(a.context.loadSymbols());}
  const snapshot=a.context.api("/api/snapshot",()=>true,"snapshot");
  a.setPrice("119");
  try{
    await a.context.loadQuotes();
    assert.equal(a.get("selected-price").textContent,"119.00","RAM quotes waited behind chart DB work");
  }finally{release();}
  await Promise.all([held,snapshot,...pending]);
  const searches=a.calls.filter(c=>c.path.includes("search=S"));
  assert.equal(searches.length,1,"superseded pending requests reached DB admission");
  assert.ok(searches[0].path.includes("search=S99"));
  assert.equal(a.busyRejections,0);
  assert.equal(a.maxDbInFlight,1);
});


test("a failed empty symbol request stays an error across periodic renders and recovers",async()=>{
  const a=await app({offline:true});
  for(const tick of a.intervals)tick();
  assert.match(a.get("symbols").children[0].textContent,/ulaşılamadı/);
  assert.doesNotMatch(a.get("symbols").children[0].textContent,/Eşleşen sembol yok/);
  a.setOffline(false);a.setMembers([]);await a.context.loadSymbols();
  for(const tick of a.intervals)tick();
  assert.match(a.get("symbols").children[0].textContent,/Eşleşen sembol yok/);
  a.setMembers(["ETHUSDT"]);await a.context.loadSymbols();
  assert.equal(a.get("symbols").children[0].className,"symbol-row active");
});

test("a debounced search remains loading until a successful response proves empty",async()=>{
  const a=await app();
  a.get("search").value="MISSING";a.get("search").events.input();
  for(const tick of a.intervals)tick();
  assert.match(a.get("symbols").children[0].textContent,/yükleniyor/);
  a.setMembers([]);await a.context.loadSymbols();
  assert.match(a.get("symbols").children[0].textContent,/Eşleşen sembol yok/);
});

test("execution prerequisites without an armed candidate never claim a trigger is watched",async()=>{
  const a=await app();
  a.data.execution_readiness={ready:true,entries_enabled:true,trial_enabled:true,reason:"READY",blockers:[]};
  a.data.analysis_readiness={ready:true,reason:"WAITING_CLOSED_BAR"};
  a.data.sections.workers={status:"OK",data:[{service:"quant_core",freshness:"OK",analysis_ready:true,reason:"WAITING_CLOSED_BAR",regime_label:"RANGE",evaluated_count:0,symbol_results:[]}]};
  a.context.render(a.data);
  assert.doesNotMatch(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
  a.data.sections.candidates={status:"OK",data:[{state:"WATCHING",freshness:"OK"}]};
  a.context.render(a.data);assert.doesNotMatch(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
  a.data.sections.candidates.data[0].state="ARMED";
  a.context.render(a.data);assert.match(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
  a.data.sections.candidates.data[0].freshness="STALE";
  a.context.render(a.data);assert.doesNotMatch(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
});

test("disorder explains strategy incompatibility without manufacturing an evaluation",async()=>{
  const a=await app();
  a.data.execution_readiness={ready:true,entries_enabled:true,reason:"READY",blockers:[]};a.data.analysis_readiness={ready:true};
  a.data.sections.workers={status:"OK",data:[{service:"quant_core",freshness:"OK",analysis_ready:true,reason:"WAITING_CLOSED_BAR",regime_label:"DISORDER",evaluated_count:0,symbol_results:[]}]};
  a.context.render(a.data);
  assert.match(a.get("algorithm-headline").textContent,/Stratejiler.*uygun değil/);
  assert.doesNotMatch(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
  assert.match(a.get("evaluated-count").textContent,/^0 /);
});

test("cycle, new analysis, source and strategy evaluation have distinct evidence times",async()=>{
  const a=await app(),at="2026-11-03T01:00:00Z",analysis="2026-11-03T00:58:00Z",source="2026-11-03T00:44:59.999Z",evaluation="2026-11-03T00:59:00Z";
  a.data.sections.workers={status:"OK",data:[{service:"quant_core",freshness:"OK",last_success_at:at,last_analysis_at:analysis,last_analysis_source_at:source,last_evaluation_at:evaluation,evaluated_count:0,selected_deep_count:8,ready_deep_count:3,input_rejections:[{symbol:"ETHUSDT",reason:"BOOK_UNAVAILABLE"}]}]};
  a.context.render(a.data);
  assert.equal(a.get("cycle-time").textContent,"04:00:00 TRT");
  assert.equal(a.get("analysis-time").textContent,"03:58:00 TRT");
  assert.equal(a.get("analysis-source-time").textContent,"03:44:59 TRT");
  assert.equal(a.get("evaluation-time").textContent,"03:59:00 TRT");
  const pipeline=a.get("pipeline").children.map(r=>r.children.map(c=>c.textContent).join(" ")).join(" ");
  assert.match(pipeline,/Seçili derin 8/);assert.match(pipeline,/Hazır derin 3/);
  const content=element=>element.textContent+" "+element.children.map(content).join(" ");
  const rejected=content(a.get("input-rejections"));
  assert.match(rejected,/ETHUSDT/);assert.match(rejected,/Emir defteri/);
});


test("an armed strategy evaluation only claims trigger watching while current and compatible",async()=>{
  const a=await app();
  a.data.execution_readiness={ready:true,entries_enabled:true,reason:"READY",blockers:[]};a.data.analysis_readiness={ready:true};
  const quant={service:"quant_core",freshness:"OK",analysis_ready:true,reason:"ANALYZED",regime_label:"RANGE",evaluated_count:1,symbol_results:[{symbol:"BTCUSDT",strategy:"REVERSAL",state:"ARMED"}]};
  a.data.sections.workers={status:"OK",data:[quant]};a.context.render(a.data);
  assert.match(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
  quant.evaluated_count=0;a.context.render(a.data);
  assert.doesNotMatch(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
  quant.evaluated_count=1;quant.freshness="STALE";a.context.render(a.data);
  assert.doesNotMatch(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
});


test("missing input rejection telemetry is unknown rather than a zero rejection measurement",async()=>{
  const a=await app();
  a.data.sections.workers={status:"OK",data:[{service:"quant_core",freshness:"OK"}]};
  a.context.render(a.data);
  assert.match(a.get("input-rejections").children[0].textContent,/ölçümü bekleniyor/);
});

test("last completed strategy results stay visible through empty cycles and transport loss",async()=>{
  const a=await app();
  const last={as_of:"2026-11-03T00:59:00Z",evaluated_count:2,reason_counts:{SPREAD_GATE:2},symbol_results:[{symbol:"BTCUSDT",strategy:"REVERSAL",side:"LONG",state:"ARMED",reasons:["SPREAD_GATE"]}],input_rejections:[{symbol:"ETHUSDT",reason:"BOOK_UNAVAILABLE"}]};
  const quant={service:"quant_core",freshness:"OK",analysis_ready:true,regime_label:"RANGE",evaluated_count:0,symbol_results:[],reason_counts:{},input_rejections:[],last_input_check:{as_of:last.as_of,input_rejections:last.input_rejections},last_evaluation:last};
  a.data.sections.workers={status:"OK",data:[quant]};
  a.data.execution_readiness={ready:true,entries_enabled:true,reason:"READY",blockers:[]};
  a.data.analysis_readiness={ready:true};
  const content=e=>e.textContent+" "+e.children.map(content).join(" ");
  a.context.render(a.data);
  assert.match(content(a.get("analysis-results")),/BTCUSDT/);
  assert.match(content(a.get("rejection-reasons")),/Alış ve satış farkı yüksek.*2/);
  assert.match(content(a.get("input-rejections")),/ETHUSDT/);
  assert.match(a.get("evaluation-note").textContent,/03:59:00 TRT/);
  assert.doesNotMatch(a.get("algorithm-headline").textContent,/Tetik izleniyor/);
  assert.equal(a.get("decision").textContent,"NO_TRADE");

  a.context.stale("Disconnected");
  assert.match(content(a.get("analysis-results")),/BTCUSDT/);
  assert.match(content(a.get("rejection-reasons")),/Alış ve satış farkı yüksek/);
  assert.match(content(a.get("input-rejections")),/ETHUSDT/);
  assert.match(a.get("evaluation-note").textContent,/Güncel değil/);
  assert.equal(a.get("decision").textContent,"NO_TRADE");

  quant.last_evaluation={...last,as_of:"2026-11-03T01:01:00Z",reason_counts:{LIQUIDITY_GATE:1},symbol_results:[{symbol:"SOLUSDT",strategy:"MOMENTUM",side:"SHORT",state:"NO_TRADE",reasons:["LIQUIDITY_GATE"]}],input_rejections:[]};
  quant.last_input_check={as_of:"2026-11-03T01:01:00Z",input_rejections:[]};
  a.pushSnapshot();
  assert.match(content(a.get("analysis-results")),/SOLUSDT/);
  assert.doesNotMatch(content(a.get("analysis-results")),/BTCUSDT/);
  assert.match(content(a.get("rejection-reasons")),/likidite yetersiz/);
  assert.doesNotMatch(content(a.get("input-rejections")),/ETHUSDT/);
  assert.match(a.get("evaluation-note").textContent,/04:01:00 TRT/);
});
