"use strict";
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname,'../../src/tradeforge/dashboard/static/demo_status.js'),'utf8');

class FakeNode {
  constructor(tag='div',text='') {
    this.tag=tag;this.textContent=text;this.children=[];this.parentElement=null;
    this.className='';this.removed=false;this.attributes={};
    this.classList={toggle(name,enabled){this[name]=enabled;}};
  }
  append(...nodes){for(const n of nodes){this.children.push(n);n.parentElement=this;}}
  replaceChildren(...nodes){this.children=[];this.append(...nodes);}
  setAttribute(k,v){this.attributes[k]=v;}
  remove(){this.removed=true;if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(x=>x!==this);}
  querySelectorAll(selector){
    let result=[];
    function walk(node){for(const c of node.children){
      if(selector==='.health-row'&&c.className==='health-row')result.push(c);
      walk(c);
    }}walk(this);return result;
  }
  querySelector(selector){
    if(selector==='span')return this.children.find(x=>x.tag==='span')||null;
    if(selector==='.tf-pretrade-preview')return this.children.find(x=>x.className==='tf-pretrade-preview')||null;
    return null;
  }
}
function healthRow(value){const n=new FakeNode();n.className='health-row';n.append(new FakeNode('span',value));return n;}
const ids={};
for(const name of ['runtime-mode','services','data-health','engine-preview','engine-pair-note'])ids[name]=new FakeNode();
ids['runtime-mode'].textContent='Doğrulanamadı';
const badge=new FakeNode('span','LIVE=false');
const handlers={};
const model={market_environment:'testnet',sections:{workers:{data:[{
  service:'quant_core',freshness:'OK',analysis_ready:true,selected_deep_count:0,
  ready_deep_count:0,regime_label:'DISORDER',last_evaluation:null
}]},opportunities:{data:[
  {symbol:'BTCUSDT',score:0.75,freshness:'OK'},
  {symbol:'ETHUSDT',score:0.64,freshness:'STALE'}
]}}};
const context=vm.createContext({
  snapshot:model,
  document:{getElementById:id=>ids[id]||null,querySelector:s=>s==='.rail-bottom .badge'?badge:null,
    createElement:t=>new FakeNode(t)},
  MutationObserver:class{observe(){}},
  window:{addEventListener:(name,fn)=>{handlers[name]=fn;}},
  queueMicrotask:fn=>fn(),
});
vm.runInContext(source,context);
assert.equal(ids['runtime-mode'].textContent,'TESTNET · EMİR PASİF');
assert.equal(badge.removed,true);
assert.match(ids['engine-pair-note'].textContent,/derin izleme.*0/);
assert.match(ids['engine-pair-note'].textContent,/DISORDER/);
let preview=ids['engine-preview'].querySelector('.tf-pretrade-preview');
assert.ok(preview);
assert.match(preview.children[0].textContent,/Strateji değerlendirmesi değil/);
const table=preview.children[1];
assert.equal(table.tag,'table');
assert.equal(table.children[1].children.length,1); // stale ETH suppressed
assert.equal(table.children[1].children[0].children[0].textContent,'BTCUSDT');
assert.ok(!source.includes('tradeforge:demo-account'));
assert.ok(!source.includes('manual_orders_available'));
assert.ok(!source.includes('fetch('));
assert.ok(!source.includes('POST'));
console.log('autonomous terminal display assertions passed');
