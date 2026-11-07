"use strict";
const assert=require("node:assert/strict");
const {test}=require("node:test");
const {runInNewContext}=require("node:vm");
const {readFileSync}=require("node:fs");
const {resolve}=require("node:path");
const source=readFileSync(resolve(__dirname,"../../src/tradeforge/dashboard/static/realtime.js"),"utf8");
function harness(){
 const sockets=[],timers=new Map(),intervals=[],snapshots=[],sessions=[],updates=[],lost=[];let timerId=0;
 class Socket{
  static CONNECTING=0;static OPEN=1;
  constructor(url){this.url=url;this.readyState=0;this.handlers={};this.sent=[];sockets.push(this);}
  addEventListener(name,fn){this.handlers[name]=fn;}send(raw){this.sent.push(JSON.parse(raw));}
  close(code,reason){this.readyState=3;this.closed={code,reason};this.fire("close");}
  fire(name,data={}){this.handlers[name]?.(data);}frame(data){this.fire("message",{data:JSON.stringify(data)});}
 }
 const scope={document:{addEventListener(){}},location:{protocol:"https:",host:"terminal.example"},WebSocket:Socket,URL,Date,Math,JSON,Promise,
  setTimeout:fn=>{timers.set(++timerId,fn);return timerId;},clearTimeout:id=>timers.delete(id),setInterval:fn=>intervals.push(fn),fetch(){throw new Error("AJAX forbidden");}};
 scope.window=scope;runInNewContext(source,scope);assert.equal(typeof scope.DashboardSocket,"function");
 const client=new scope.DashboardSocket({onSession:data=>sessions.push(data),onSnapshot:data=>snapshots.push(data),onUpdate:data=>updates.push(data),onQuotes(){},onDisconnect:reason=>lost.push(reason)});client.connect();
 function ready(socket=sockets.at(-1)){socket.readyState=1;socket.fire("open");socket.frame({seq:1,type:"hello",protocol:2,session:{subject:"observer",scopes:["read"]}});socket.frame({seq:2,type:"snapshot",data:{status:"DEGRADED"}});return socket;}
 return {client,sockets,timers,intervals,snapshots,sessions,updates,lost,ready};
}
test("one WSS connection multiplexes reads and pushes without AJAX",async()=>{
 const h=harness(),ws=h.ready();assert.equal(ws.url,"wss://terminal.example/api/realtime");
 const read=h.client.request("/api/market/symbols?search=ETH&offset=0"),frame=ws.sent.at(-1);
 assert.deepEqual(frame,{type:"request",id:1,op:"symbols",params:{search:"ETH",offset:0}});
 ws.frame({seq:3,type:"response",id:frame.id,ok:true,data:{items:[{symbol:"ETHUSDT"}]}});assert.equal((await read).items[0].symbol,"ETHUSDT");
 ws.frame({seq:4,type:"symbols",id:frame.id,ok:true,data:{items:[{symbol:"SOLUSDT"}]}});assert.equal(h.updates[0].data.items[0].symbol,"SOLUSDT");
 for(const tick of h.intervals)tick();assert.equal(ws.sent.length,1,"timers must not request backend data");assert.equal(h.sockets.length,1);
});
test("sequence loss immediately invalidates state and reconnect never replays requests",async()=>{
 const h=harness(),ws=h.ready(),read=h.client.request("/api/market/symbols?search=ETH&offset=0");
 const rejected=assert.rejects(read,/CONNECTION_LOST/);ws.frame({seq:5,type:"heartbeat"});await rejected;
 assert.equal(h.lost.length,1);assert.equal(ws.closed.code,4000);for(const retry of [...h.timers.values()])retry();assert.equal(h.sockets.length,2);h.ready();assert.equal(h.sockets[1].sent.length,0);
});
test("superseded chart subscriptions cannot replace current state",async()=>{
 const h=harness(),ws=h.ready(),old=h.client.request("/api/market/chart?symbol=BTCUSDT&timeframe=15m"),oldId=ws.sent.at(-1).id;
 const latest=h.client.request("/api/market/chart?symbol=ETHUSDT&timeframe=15m"),latestId=ws.sent.at(-1).id;
 ws.frame({seq:3,type:"response",id:oldId,ok:true,data:{bars:[]}});ws.frame({seq:4,type:"response",id:latestId,ok:true,data:{bars:[]}});await Promise.all([old,latest]);
 ws.frame({seq:5,type:"chart",id:oldId,ok:true,data:{symbol:"BTCUSDT"}});ws.frame({seq:6,type:"chart",id:latestId,ok:true,data:{symbol:"ETHUSDT"}});assert.equal(h.updates.length,1);assert.equal(h.updates[0].params.symbol,"ETHUSDT");
 ws.close(1001,"restart");for(const retry of [...h.timers.values()])retry();h.ready();ws.frame({seq:7,type:"snapshot",data:{status:"OLD"}});assert.equal(h.snapshots.at(-1).status,"DEGRADED");
});

test("real dashboard scripts use no fetch on bootstrap, actions, timers or reconnect",async()=>{
 const elements=new Map(),sockets=[],fetches=[],timers=new Map(),intervals=[];let timerId=0;
 class Element{constructor(){this.textContent="";this.value="";this.children=[];this.dataset={};this.events={};this.classList={toggle(){}};}replaceChildren(...nodes){this.children=nodes;}append(...nodes){this.children.push(...nodes);}setAttribute(){}addEventListener(name,fn){this.events[name]=fn;}}
 const get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
 const state={status:"DEGRADED",sections:{risk:{status:"OK",data:[{version:7}]}},service_health:{}};
 class Socket{
  static CONNECTING=0;static OPEN=1;
  constructor(url){this.url=url;this.readyState=0;this.handlers={};this.sent=[];this.seq=0;sockets.push(this);setImmediate(()=>{this.readyState=1;this.frame({type:"hello",protocol:2,session:{scopes:["read","control"]}});this.frame({type:"snapshot",data:state});});}
  addEventListener(name,fn){this.handlers[name]=fn;}frame(frame){this.handlers.message?.({data:JSON.stringify({seq:++this.seq,...frame})});}
  send(raw){const req=JSON.parse(raw);this.sent.push(req);if(req.op==="quotes"&&this.holdQuotes)return;setImmediate(()=>{const data=req.op==="symbols"?{items:[{symbol:"BTCUSDT"}],has_more:false}:req.op==="chart"?{symbol:req.params.symbol,timeframe:req.params.timeframe,bars:[]}:req.op==="quotes"?{transport:"OK",items:[{symbol:"BTCUSDT",price:"101",source_at:new Date().toISOString()}]}:state;this.frame({type:"response",id:req.id,ok:true,data});});}
  close(){this.readyState=3;this.handlers.close?.();}
 }
 const scope={document:{getElementById:get,createElement:()=>new Element(),querySelectorAll:()=>[],addEventListener(){}},TerminalChart:class{set(){}draw(){}},location:{protocol:"https:",host:"terminal.example"},WebSocket:Socket,URL,Date,Math,JSON,Promise,AbortController,AbortSignal,performance:{now:()=>1},crypto:{randomUUID:()=>"00000000-0000-4000-8000-000000000001"},confirm:()=>true,
  setTimeout:fn=>{timers.set(++timerId,fn);return timerId;},clearTimeout:id=>timers.delete(id),setInterval:fn=>intervals.push(fn),fetch:path=>{fetches.push(path);return Promise.reject(new Error("AJAX forbidden"));}};
 scope.window=scope;runInNewContext(source,scope);runInNewContext(readFileSync(resolve(__dirname,"../../src/tradeforge/dashboard/static/app.js"),"utf8"),scope);
 const settle=async()=>{for(let i=0;i<25;i++)await new Promise(setImmediate);};await settle();
 assert.equal(fetches.length,0,"initial state must arrive over the socket");assert.equal(sockets.length,1);
 assert.equal(get("selected-symbol").textContent,"BTCUSDT");assert.equal(get("selected-price").textContent,"101.00");
 const symbolsSubscription=sockets[0].sent.find(frame=>frame.op==="symbols"),count=sockets[0].sent.length;
 sockets[0].frame({type:"symbols",id:symbolsSubscription.id,ok:true,data:{items:[{symbol:"BTCUSDT"}],has_more:false}});await settle();
 assert.equal(sockets[0].sent.length,count,"unchanged pushed membership must not create periodic requests");
 sockets[0].holdQuotes=true;const held=scope.loadQuotes();sockets[0].holdQuotes=false;
 await scope.selectSymbol("ETHUSDT");await held;await settle();
 assert.ok(sockets[0].sent.filter(frame=>frame.op==="quotes").at(-1).params.symbols.includes("ETHUSDT"),"aborting a pending quote request must install the new persistent subscription");
 await scope.selectSymbol("BTCUSDT");
 await scope.loadChart();get("search").value="ETH";await scope.loadSymbols();await scope.api("/api/snapshot",()=>true,"snapshot");
 assert.ok(sockets[0].sent.some(frame=>frame.op==="snapshot"));
 const before=sockets[0].sent.length;for(const tick of intervals)tick();await settle();assert.equal(sockets[0].sent.length,before,"timers must not poll even over WebSocket");
 sockets[0].close();assert.equal(get("selected-price").textContent,"101.00 · eski");
 for(const retry of [...timers.values()])retry();await settle();assert.equal(sockets.length,2);assert.ok(sockets[1].sent.some(frame=>frame.op==="chart"));assert.ok(!sockets[1].sent.some(frame=>frame.op==="snapshot"));assert.equal(fetches.length,0);
});
