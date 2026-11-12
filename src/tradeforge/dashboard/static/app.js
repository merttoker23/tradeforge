"use strict";
const $ = id => document.getElementById(id);
let snapshot=null, generation=0, receivedAt=0, backendConnected=false, socketState="CONNECTING";
let selected="", timeframe="15m", symbols=[], offset=0, filter="all", tab="positions", searchTimer=null, chartGeneration=0;
let engineFilter="all", engineSearch="";
let chartMetricsAt=0, chartHasData=false, symbolStatus="loading";
let canControl=false, transport=null;
const requests=new Map(), pendingReads=new Map(), dbLanes=new Set(["snapshot","symbols","chart"]);
let drainingReads=false;
let quotePending=null,quoteGeneration=0,quoteTransport="UNAVAILABLE";
const favorites=new Set(), visited=new Map();
const chart=new TerminalChart($("candles"),$("chart-empty"));
const labels={OK:"Güncel",UNKNOWN:"Veri bekleniyor",STALE:"Güncelliği doğrulanamadı",DEGRADED:"Kısmi veri",NOT_READY:"Hazır değil",NOT_RUNNING:"Çalışmıyor",WAITING:"Bekliyor",FAIL_CLOSED:"Girişler durduruldu",NEW:"Bekliyor",FILLED:"Gerçekleşti",CANCELED:"İptal",LONG:"LONG",SHORT:"SHORT",NO_TRADE:"NO_TRADE",PARTIALLY_FILLED:"Kısmi gerçekleşme"};
const reasons={API_KEY_INVALID:"Demo anahtarı kabul edilmedi. Sunucunun Demo kimlik dosyalarını denetleyin.",API_KEY_PERMISSION_OR_IP_DENIED:"Demo anahtarının izinlerini veya IP kısıtını denetleyin.",DEMO_CREDENTIALS_MISSING:"Demo kimlik dosyalarını sunucuda yapılandırın.",DEMO_SECRET_FILE_UNSAFE:"Sunucunun özel Demo kimlik dosyaları doğrulanamadı.",EXCHANGE_AUTH_REJECTED:"Demo anahtarı veya izinleri kabul edilmedi. Yeni Demo anahtarlarını denetleyin.",EXCHANGE_TRANSPORT_UNKNOWN:"Binance Demo’ya ulaşılamadı. Bağlantı yeniden denetlenecek.",CLOCK_UNSAFE:"Sunucunun saati Binance Demo ile doğrulanamadı. Sistem Sağlığı ayrıntılarını denetleyin.",CLOCK_PROBE_SLOW:"Binance Demo saat kontrolü gecikti. Bağlantı yeniden denetlenecek.",RATE_LIMITED:"Borsa istek sınırına ulaşıldı; bağlantı beklemede.",DEMO_RESPONSE_INVALID:"Demo hesap yanıtı doğrulanamadı.",KEY_PERMISSIONS_UNVERIFIED:"Anahtar yetkileri API üzerinden tam doğrulanamadı.",EXECUTION_NOT_RUNNING:"Otomatik emir motoru etkin değil; işlem açılmaz.",INSUFFICIENT_EVIDENCE:"Karar için yeterli doğrulanmış piyasa kanıtı yok.",BTC_HISTORY_MISSING:"Göreceli güç için yeterli eşzamanlı BTC geçmişi yok."};
function value(x){if(x===null||x===undefined)return "—"; if(Array.isArray(x))return x.length?x.map(value).join(" · "):"—";return labels[x]||String(x);}
function money(x){if(x===null||x===undefined||x===""||!Number.isFinite(Number(x)))return "—";return Number(x).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:8});}
function node(tag,text,className){const e=document.createElement(tag);e.textContent=text;if(className)e.className=className;return e;}
function screen(name){document.querySelectorAll(".screen").forEach(e=>e.hidden=e.id!==name+"-screen");document.querySelectorAll("[data-screen]").forEach(e=>e.classList.toggle("active",e.dataset.screen===name));$("screen-label").textContent={overview:"Genel bakış",terminal:"Piyasa terminali",health:"Sistem sağlığı",binance:"Binance API Ayarları"}[name];if(name==="terminal")chart.draw();}
function table(target,rows,columns,empty="Henüz doğrulanmış kayıt yok.",limit=100){
  const root=$(target);root.replaceChildren();if(!rows?.length){root.append(node("p",empty,"empty"));return;}
  const t=document.createElement("table"),head=document.createElement("thead"),hr=document.createElement("tr"),body=document.createElement("tbody");
  columns.forEach(([,title])=>{const th=node("th",title);th.scope="col";hr.append(th);});head.append(hr);t.append(head);
  rows.slice(0,limit).forEach(row=>{const tr=document.createElement("tr");columns.forEach(([key])=>{const td=node("td",value(row[key]));if(["realized_pnl","unrealized_pnl"].includes(key)&&row[key]!=null)td.className=Number(row[key])>=0?"positive":"negative";tr.append(td);});body.append(tr);});t.append(body);root.append(t);
}
function clearChart(){chartHasData=false;chart.set({bars:[]});$("chart-time").textContent="—";clearChartMetrics();}
function clearChartMetrics(){chartMetricsAt=0;$("selected-volatility").textContent="—";$("selected-strength").textContent="—";}
function expireChartMetrics(){const age=Date.now()-chartMetricsAt;if(!chartMetricsAt||age<0||age>=1800000)clearChartMetrics();}
function stale(message){backendConnected=false;clearChartMetrics();if(snapshot){snapshot.status="STALE";Object.keys(snapshot.service_health||{}).forEach(k=>snapshot.service_health[k]="UNKNOWN");Object.values(snapshot.sections||{}).forEach(section=>{section.status="STALE";});render(snapshot);}$("banner").hidden=false;$("banner").className="banner stale";$("banner").textContent=message;}
function expireSources(data) {
  let expired=false;
  const streams=data.sections?.streams,universe=data.sections?.universe?.data?.[0],measurement=streams?.data?.[0];
  if(universe)universe.stream_coverage=null;
  if(measurement&&["OK","DEGRADED"].includes(streams.status)&&measurement.measured===true&&measurement.clock_safe===true&&Array.isArray(measurement.coverage_deadlines)){
    const previous=measurement.covered;measurement.covered=measurement.coverage_deadlines.filter(value=>Date.parse(value)>Date.now()).length;
    if(previous!==measurement.covered)expired=true;
    if(measurement.covered<measurement.eligible&&streams.status==="OK"){measurement.freshness=streams.status="DEGRADED";expired=true;}
    if(universe&&data.sections.universe.status==="OK"&&measurement.universe_version===universe.version_hash&&universe.eligible===measurement.eligible)universe.stream_coverage=measurement.covered/measurement.eligible;
  }
  Object.values(data.sections||{}).forEach(section=>(section.data||[]).forEach(row=>{if(row.fresh_until&&Date.parse(row.fresh_until)<=Date.now()){if(row.freshness!=="STALE")expired=true;row.freshness="STALE";section.status="STALE";}}));
  if(expired&&data.status!=="STALE")data.status="DEGRADED";
  if(data.sections?.demo?.status==="STALE"&&data.demo_readiness){data.demo_readiness.status="STALE";data.demo_readiness.orders_enabled=false;data.demo_readiness.reason_codes=["DEMO_PREFLIGHT_STALE"];}
  if(expired){Object.keys(data.data_readiness||{}).forEach(name=>data.data_readiness[name]=data.sections?.[name]?.status||"UNKNOWN");(data.sections?.workers?.data||[]).forEach(row=>{if(data.service_health)data.service_health[row.service]=row.freshness;});if(data.sections?.streams?.status==="STALE"||data.sections?.universe?.status!=="OK")(data.sections?.universe?.data||[]).forEach(row=>row.stream_coverage=null);}
  const workers=data.sections?.workers?.data||[];
  if(workers.some(w=>w.service==="execution_core"&&w.freshness!=="OK")&&data.execution_readiness)data.execution_readiness={...data.execution_readiness,ready:false,entries_enabled:false,reason:"DISPATCH_STALE",blockers:["DISPATCH_STALE"]};
  if(workers.some(w=>w.service==="quant_core"&&w.freshness!=="OK")&&data.analysis_readiness)data.analysis_readiness={ready:false,reason:"QUANT_TICK_STALE"};
  return expired;
}
const healthSources={
  universe:["Parite evreni","Binance sözleşme ve uygunluk doğrulaması"],
  streams:["Piyasa veri akışı","WebSocket kapsamı, saat tutarlılığı ve veri tazeliği"],
  regime:["Piyasa rejimi","Trend, yatay piyasa ve volatilite değerlendirmesi"],
  opportunities:["Fırsat adayları","Tarama sonucu üretilen ve saklanan adaylar"],
};
const healthServices={market_gateway:"Piyasa veri servisi",quant_core:"Analiz motoru",execution_core:"Otomatik yürütme (Quant Core)",dashboard:"Gösterge paneli"};
const healthStatusText={OK:"Güncel",DEGRADED:"Kısmi veri",STALE:"Güncel değil",UNKNOWN:"Ölçüm bekleniyor",NOT_READY:"Hazır değil",NOT_RUNNING:"Çalışmıyor",WAITING:"Bekliyor",FAIL_CLOSED:"Girişler kapalı"};
function renderHealth(data){
  const fresh=backendConnected&&data.status!=="STALE",sections=data.sections||{},root=$("data-health"),serviceRoot=$("services");
  root.replaceChildren();serviceRoot.replaceChildren();
  const headline=node("div","","ov-health-intro");headline.append(node("strong","Veri hazırlığı ve kalite kontrolü"),node("p","Her satır ayrı bir kanıt kaynağıdır. Ölçüm bekleniyor ifadesi hata olmadığı anlamına gelmez."));root.append(headline);
  const reported=data.data_readiness||{},names=[...new Set([...Object.keys(healthSources),...Object.keys(reported).filter(name=>!healthServices[name])])];
  for(const name of names){
    const sectionStatus=sections[name]?.status,status=fresh?(sectionStatus||reported[name]||"UNKNOWN"):"UNKNOWN";
    const [label,description]=healthSources[name]||[name,"Kaynak sağlık durumu"];
    const row=node("div","","health-row ov-health-row"),info=node("span","","ov-health-name");info.append(node("strong",label),node("small",description));
    const tag=node("span",healthStatusText[status]||"Ölçüm bekleniyor","ov-health-tag "+ovClass(status));
    row.append(info,tag);root.append(row);
  }
  const workers=sections.workers?.data||[];
  for(const [service,status] of Object.entries(Object.fromEntries([...Object.entries(data.service_health||{}),...workers.filter(row=>row&&row.service).map(row=>[row.service,row.freshness||"UNKNOWN"])]))){
    const worker=workers.find(item=>item.service===service),state=fresh?worker?.freshness||status:"UNKNOWN";
    const row=node("div","","health-row ov-health-row"),info=node("span","","ov-health-name");
    info.append(node("strong",healthServices[service]||service),node("small",worker&&fresh?[
      worker.healthy===true?"Servis sağlığı: olumlu":worker.healthy===false?"Servis sağlığı: sorun bildirildi":"Sağlık kanıtı doğrulanmadı",
      worker.data_ready===true?"Veri hazır":worker.data_ready===false?"Veri hazır değil":"Veri durumu bilinmiyor",
      worker.analysis_ready===true?"Analiz hazır":worker.analysis_ready===false?"Analiz hazır değil":null,
      worker.execution_ready===true?"Emir kapısı hazır":worker.execution_ready===false?"Emir kapısı hazır değil":null,
      worker.stage?"Aşama: "+worker.stage:null,
    ].filter(Boolean).join(" · ")+" · "+service:"Son servis ölçümü bekleniyor · "+service));
    row.append(info,node("span",healthStatusText[state]||"Ölçüm bekleniyor","ov-health-tag "+ovClass(state)));serviceRoot.append(row);
  }
  if(!serviceRoot.children.length)serviceRoot.append(node("p","Servis sağlık ölçümü henüz alınmadı.","ov-placeholder"));
}

function render(data) {
  expireSources(data);snapshot=data;const sections=data.sections||{},demo=sections.demo?.data?.[0],fresh=sections.demo?.status==="OK"&&data.status!=="STALE"&&backendConnected,verified=demo?.connection==="OK"&&demo?.account==="OK"&&demo?.balance==="OK"&&demo?.positions==="OK"&&demo?.equity!=null&&demo?.available_balance!=null,account=verified?demo?.terminal:null;
  $("equity").textContent=verified?money(demo?.equity):"—";$("available").textContent=verified?money(demo?.available_balance):"—";$("daily-pnl").textContent=money(account?.daily_pnl);$("total-pnl").textContent=money(account?.total_pnl);$("position-count").textContent=verified?value(demo?.position_count):"—";$("total-risk").textContent=account?.total_risk!=null?money(account.total_risk):"Ölçülemedi";
  $("account-note").textContent=verified?(fresh?"Binance Demo · güncel":"Son doğrulanmış Demo verisi · güncel değil"):"Demo hesap verisi henüz doğrulanmadı";$("demo-connection").textContent=fresh?"● Demo bağlı":verified?"● Son doğrulanmış veri · eski":"● Demo veri bekleniyor";$("demo-connection").className="connection"+(fresh?" ok":"");
  const executionState=data.execution_readiness||{};
  $("execution-summary").textContent=!backendConnected||data.status==="STALE"?"Bağlantı bekleniyor":
    executionState.ready&&executionState.entries_enabled?"Otomatik emir açık":
    executionState.entries_enabled?"Koruma / doğrulama bekleniyor":"Yeni emir kapalı";
  $("execution-detail").textContent=executionState.ready&&executionState.entries_enabled?
    "Yalnızca uygun sinyal ve doğrulanmış risk koşullarında emir otomatik gönderilir.":
    "Son neden: "+reasonText(executionState.reason||"DISPATCH_NOT_OBSERVED")+" · Canlı hesap emirleri kapalı.";
  const codes=data.demo_readiness?.reason_codes||[],action=codes.find(c=>reasons[c]&&c!=="EXECUTION_NOT_RUNNING"&&c!=="KEY_PERMISSIONS_UNVERIFIED");
  $("banner").hidden=!action&&data.status!=="STALE";$("banner").className="banner"+(data.status==="STALE"?" stale":"");$("banner").textContent=data.status==="STALE"?"Sunucu verisi geçici olarak alınamıyor; son doğrulanmış değerler korunuyor.":action?reasons[action]:"";
  const columns={positions:[["symbol","SEMBOL"],["side","YÖN"],["qty","MİKTAR"],["entry_price","GİRİŞ"],["mark_price","DEMO MARK"],["unrealized_pnl","AÇIK PNL"]],orders:[["symbol","SEMBOL"],["side","YÖN"],["kind","TÜR"],["qty","MİKTAR"],["price","FİYAT"],["status","DURUM"]],stops:[["symbol","SEMBOL"],["kind","TÜR"],["qty","MİKTAR"],["stop_price","TETİK"],["status","DURUM"]],history:[["symbol","SEMBOL"],["side","YÖN"],["price","GERÇEKLEŞME"],["qty","MİKTAR"],["realized_pnl","PNL"],["commission","KOMİSYON"],["commission_asset","BİRİM"],["as_of","UTC"]]};
  table("account-records",account?.[tab],columns[tab],fresh?"Bu görünümde doğrulanmış Demo kaydı bulunmuyor.":"Demo bağlantısı doğrulanmadı; hesap verisi gösterilemiyor.");$("demo-record-note").textContent="Binance Futures Demo · "+(fresh?"Kaynak: "+value(demo.as_of):"Güncel hesap kanıtı bekleniyor")+(tab==="history"?" · Son 5 ilgili sembol / en fazla 100 işlem; tam hesap geçmişi değildir.":"");
  renderOverview(data);renderDecision();renderHealth(data);
  const marketState=sections.streams?.data?.[0]||{};
  const current=backendConnected&&data.status!=="STALE";
  const market=(sections.workers?.data||[]).find(worker=>worker.service==="market_gateway");
  const streamFresh=current&&["OK","DEGRADED"].includes(sections.streams?.status)&&marketState.clock_safe===true;
  if ($("binance-health")) {
    $("binance-health").replaceChildren();
    const liveAnalysis=current?data.live_analysis:null,links=liveAnalysis?.connections||{};
    const wsState={CONNECTED:"Bağlı",NO_DATA:"Veri yok",UNAVAILABLE:"Kapalı",UNKNOWN:"Doğrulanamadı"}[links.binance_websocket]||"Doğrulanamadı";
    const restState={CONNECTED:"Bağlı",RATE_LIMITED:"Limitli",STALE:"Koptu",NOT_SAMPLED:"Ölçülmedi",UNKNOWN:"Doğrulanamadı"}[links.binance_rest]||"Doğrulanamadı";
    const workerFailures=Object.entries(links.worker_errors||{}).map(([name,count])=>name+": "+count).join(" · ");
    const rows={"Aktif kaynak":marketState.market_environment||"Doğrulanamadı",
      "WebSocket":[wsState,links.ws_reconnects!=null?links.ws_reconnects+" yeniden":null,links.ws_failures!=null?links.ws_failures+" hata":null].filter(Boolean).join(" · "),
      "REST":[restState,links.rest_last_success_at?"son başarı "+localTime(links.rest_last_success_at):null,links.rest_failures!=null?links.rest_failures+" hata":null].filter(Boolean).join(" · "),
      "REST son sonuç":links.rest_last_reason||"Doğrulanamadı",
      "Akış durumu":streamFresh?"Saat ve worker güncel":"Doğrulanamadı",
      "Worker hataları":workerFailures||(liveAnalysis?"Kayıtlı hata yok":"Bilinmiyor"),
      "Analiz gecikmesi":liveAnalysis?secondsText(liveAnalysis.latency?.analysis_seconds):"Bilinmiyor"};
    Object.entries(rows).forEach(([name,state])=>{const r=node("div","","health-row");r.append(node("span",name),node("span",String(state)));$("binance-health").append(r);});
  }
  $("sample-time").textContent="Kaynak: "+value(data.as_of);
}
// Overview V2. Snapshot evidence comes only from the authenticated dashboard WebSocket.
// Missing or stale data is never interpreted as zero, a successful trade, or a live signal.
const overviewStageText={SCAN:"Piyasa taraması",DEEP:"Derin analiz"};
const overviewEngineText={ACTIVE:"Analiz çalışıyor",SCANNING:"Piyasa taranıyor",WAITING:"Analiz bekliyor",ERROR:"Motor hatası",DELAYED:"Veri gecikiyor",UNKNOWN:"Doğrulanamadı"};
const overviewPipelineText={SIGNAL:"Sinyal",RISK:"Risk filtresi",ORDER_QUEUE:"Emir kuyruğu",EXCHANGE_ACK:"Borsa sonucu"};
const overviewPipelineState={PRESENT:"Aday var",NONE:"Aday yok",READY:"Kontrol hazır",BLOCKED:"Risk engeli",PENDING:"İşlem bekliyor",EMPTY:"Bekleyen yok",ACK_UNKNOWN:"Onay belirsiz",REJECTED:"Reddedildi",FILLED:"Gerçekleşti",UNKNOWN:"Bilinmiyor"};
const overviewReasons={
  UNIVERSE_UNAVAILABLE:"Uygun parite listesi henüz doğrulanamadı",
  ANALYSIS_WORKER_LAG:"Analiz motorundan yeni ölçüm alınamadı",
  ANALYSIS_SOURCE_LAG:"Analizin temel aldığı mum verisi gecikiyor",
  ANALYSIS_NOT_OBSERVED:"Henüz motor analizi ölçülmedi",
  MARKET_SCAN_ONLY:"Yalnızca piyasa taraması mevcut; derin analiz tamamlanmadı",
  WAITING_CLOSED_BAR:"Yeni bir kapanmış mum bekleniyor",
  QUANT_TICK_STALE:"Analiz motoru ölçümü güncel değil",
  WAITING_HISTORY_SWEEP:"Yeni dönem fiyatları tüm çiftler için tamamlanmadı; kısmi tarama yayınlanmıyor",
  FRAME_BENCHMARK_MISSING:"BTC referans mumu henüz alınmadı; göreceli güç ölçülemiyor"
};
function ovReason(code){return overviewReasons[code]||reasonText(code)||"Henüz gerekçe iletilmedi";}
function ovNum(number){return number===null||number===undefined||!Number.isFinite(Number(number))?"—":Number(number).toLocaleString("tr-TR");}
function ovPercent(number,digits=2){return number===null||number===undefined||!Number.isFinite(Number(number))?"—":(Number(number)*100).toLocaleString("tr-TR",{maximumFractionDigits:digits,minimumFractionDigits:digits})+"%";}
function ovElapsed(seconds){if(seconds===null||seconds===undefined||!Number.isFinite(Number(seconds)))return "—";return Number(seconds)<1?Math.round(Math.max(0,Number(seconds))*1000)+" ms":Math.max(0,Number(seconds)).toFixed(1)+" sn";}
function ovClass(state){return ["OK","READY","ACTIVE","FILLED","PRESENT","CONNECTED","LONG"].includes(state)?"ok":["ERROR","STALE","BLOCKED","REJECTED","ACK_UNKNOWN"].includes(state)?"bad":["DEGRADED","SCANNING","WAITING","DELAYED","SHORT","PENDING"].includes(state)?"caution":"unknown";}
function ovText(id,text){$(id).textContent=text==null?"—":String(text);}
function ovBadge(id,text,state){const target=$(id);target.textContent=text;target.className=(id==="engine-state"||id==="algorithm-status"?"status-pill ":"ov-small-badge ")+ovClass(state);}
let overviewPage=0,overviewSelected="",overviewRows=[],overviewLive=false,overviewData=null,overviewRateSample=null;
function overviewRate(data){
  const live=data.live_analysis,when=Date.parse(data.evaluated_at||data.as_of),next={when,market:live?.rates?.market_events_total,evaluation:live?.rates?.evaluations_total};
  let market=null,evaluation=null;
  if(overviewLive&&overviewRateSample&&Number.isFinite(when)&&when>overviewRateSample.when){
    const seconds=(when-overviewRateSample.when)/1000;
    for(const field of ["market","evaluation"]){
      if(next[field]===null||next[field]===undefined||overviewRateSample[field]===null||overviewRateSample[field]===undefined)continue;
      const now=Number(next[field]),before=Number(overviewRateSample[field]);
      if(Number.isFinite(now)&&Number.isFinite(before)&&now>=before){if(field==="market")market=(now-before)/seconds;else evaluation=(now-before)/seconds;}
    }
  }
  if(Number.isFinite(when))overviewRateSample=next;
  return {market,evaluation};
}
function ovRate(rate){return rate===null?"—":(rate<1?rate.toFixed(2):rate.toFixed(1))+" /sn";}
function overviewDecision(row){return row.stage!=="DEEP"?"TARAMA":row.decision||"Bekliyor";}
function overviewMatches(row){
  if(engineSearch.trim()&&!String(row.symbol||"").toUpperCase().includes(engineSearch.trim().toUpperCase()))return false;
  if(engineFilter==="deep")return row.stage==="DEEP";
  if(["long","short","no_trade"].includes(engineFilter))return row.stage==="DEEP"&&row.decision===engineFilter.toUpperCase();
  return true;
}
function overviewPairDetails(){
  const root=$("ov-detail");root.replaceChildren();const row=overviewRows.find(r=>r.symbol===overviewSelected);
  if(!row){root.append(node("p","Parite analizi alınamadı. Tarama geldiğinde ayrıntılar burada görünecek.","ov-placeholder"));ovText("ov-detail-heading","Analiz açıklaması");ovText("ov-detail-badge","Seçim bekleniyor");return;}
  ovText("ov-detail-heading",row.symbol);ovBadge("ov-detail-badge",overviewDecision(row),overviewLive?row.decision||"SCANNING":"UNKNOWN");
  const heading=node("div","","ov-detail-summary");heading.append(node("span",overviewStageText[row.stage]||"Tarama bekliyor","ov-detail-eyebrow"),node("strong",row.direction===1?"Yukarı yönlü tarama":row.direction===-1?"Aşağı yönlü tarama":row.direction===0?"Nötr tarama":"Yön ölçümü bekleniyor"));
  heading.append(node("p",row.stage==="DEEP"?"Strateji değerlendirmesi yapıldı. Karar, tek başına emir yetkisi değildir.":"Parite piyasa genelinde tarandı. Derin strateji değerlendirmesi henüz gösterilmiyor."));root.append(heading);
  const fields=[
    ["Strateji",overviewStrategies(row,strategyLabel)||"Derin analiz bekleniyor"],
    ["Volatilite",ovPercent(row.volatility)],
    ["BTC’ye göre getiri",ovPercent(row.relative_return)],
    ["Trend verimi",ovPercent(row.efficiency,0)],
    ["Alış / satış farkı",row.spread_bps==null?"—":Number(row.spread_bps).toFixed(2)+" bp"],
    ["Funding",ovPercent(row.funding,4)],
    ["Örnek sayısı",ovNum(row.sample_count)],
    ["Değerlendirme",localTime(row.analyzed_at)],
    ["Kaynak zamanı",localTime(row.as_of)]
  ];
  const grid=node("dl","","ov-detail-grid");for(const [name,detail] of fields){const item=node("div","","ov-detail-item");item.append(node("dt",name),node("dd",detail));grid.append(item);}root.append(grid);
  const reasons=node("div","","ov-why");reasons.append(node("strong","Kararın gerekçesi"));
  const notes=Array.isArray(row.reasons)&&row.reasons.length?row.reasons.map(ovReason):[row.stage==="DEEP"?"Bu değerlendirme için neden kodu iletilmedi.":"Derin strateji sonucu henüz bulunmuyor."];
  if(!overviewLive)reasons.append(node("p","Canlı bağlantı doğrulanamadı. Gösterilen kayıt güncel kabul edilmez.","ov-stale-note"));
  for(const message of notes.slice(0,8))reasons.append(node("p",message));
  root.append(reasons);
}
function renderEnginePairs(){
  const filtered=overviewRows.filter(overviewMatches),pages=Math.max(1,Math.ceil(filtered.length/25));overviewPage=Math.min(overviewPage,pages-1);
  const subset=filtered.slice(overviewPage*25,(overviewPage+1)*25),root=$("engine-pairs");
  root.replaceChildren();
  if(!filtered.length){root.append(node("p",overviewRows.length?"Bu filtreyle eşleşen analiz kaydı bulunamadı.":"Canlı parite tarama kaydı henüz alınmadı.","ov-placeholder"));}
  else{
    const tableEl=document.createElement("table");tableEl.className="ov-table";
    const thead=document.createElement("thead"),head=document.createElement("tr");
    for(const name of ["PARİTE","AŞAMA","TARAMA YÖNÜ","KARAR","STRATEJİ","VOLATİLİTE","BTC GÜCÜ","SPREAD","SON ANALİZ"]){const cell=node("th",name);cell.scope="col";head.append(cell);}thead.append(head);tableEl.append(thead);
    const body=document.createElement("tbody");
    for(const row of subset){const tr=document.createElement("tr");if(overviewSelected===row.symbol)tr.className="ov-selected";
      const selectCell=document.createElement("td"),select=node("button",row.symbol,"ov-symbol-button");select.type="button";select.setAttribute("aria-pressed",String(overviewSelected===row.symbol));select.setAttribute("aria-label",row.symbol+" analiz detayını göster");select.addEventListener("click",()=>{overviewSelected=row.symbol;renderEnginePairs();});selectCell.append(select);tr.append(selectCell);
      const cells=[[overviewStageText[row.stage]||"—",""],[row.direction===1?"↑ Pozitif":row.direction===-1?"↓ Negatif":row.direction===0?"→ Nötr":"—",row.direction===1?"positive":row.direction===-1?"negative":""],[overviewDecision(row),"ov-decision "+(overviewLive?ovClass(row.decision):"unknown")],[overviewStrategies(row,strategyShortLabel)||"—",""],[ovPercent(row.volatility),""],[ovPercent(row.relative_return),""],[row.spread_bps==null?"—":Number(row.spread_bps).toFixed(2)+" bp",""],[localTime(row.analyzed_at),""]];
      for(const [value,style] of cells)tr.append(node("td",value,style));
      tr.children[4].title=overviewStrategies(row,strategyLabel);
      body.append(tr);
    }
    tableEl.append(body);root.append(tableEl);
  }
  root.scrollTop=0;
  ovText("ov-page-summary",`${filtered.length} eşleşme · Sayfa ${overviewPage+1}/${pages}${overviewLive?"":" · son doğrulanmış veri"}`);
  $("ov-prev").disabled=overviewPage===0;$("ov-next").disabled=overviewPage>=pages-1;
  ovText("ov-count-all",overviewRows.length?`(${overviewRows.length})`:"");
  overviewPairDetails();
}
function secondsText(seconds){return ovElapsed(seconds);}
function regimeText(label){return {UNKNOWN:"Belirlenemedi",TRANSITION:"Geçiş",TREND_EXPANSION:"Trend genişlemesi",RANGE:"Yatay piyasa",VOL_COMPRESSION:"Volatilite sıkışması",DISORDER:"Düzensiz piyasa"}[label]||value(label);}
function renderDecision(){
  const rows=snapshot?.sections?.signals?.data||[],row=rows.find(r=>r.symbol===selected),fresh=row&&row.freshness==="OK"&&backendConnected&&snapshot.status!=="STALE",valid=fresh&&row.state==="APPROVED"&&["LONG","SHORT"].includes(row.side);
  const quant=snapshot?.sections?.workers?.data?.find(w=>w.service==="quant_core"),evaluation=backendConnected&&quant?.freshness==="OK"&&snapshot.status!=="STALE"?quant.symbol_results?.find(r=>r.symbol===selected):null;
  $("decision-symbol").textContent=selected||"SEÇİLİ PARİTE";$("decision").textContent=valid?row.side:fresh?stageLabel(row.state):evaluation?stageLabel(evaluation.state):"NO_TRADE";$("decision").className="decision"+(valid?" "+row.side.toLowerCase():"");
  $("decision-explanation").textContent=valid?(row.execution_authority==="TESTNET_TRIAL"?"Testnet denemesi · UNPROVEN. Strateji istatistiksel olarak doğrulanmadı. Risk ve koruma kapıları zorunludur.":"Güncel algoritma kararı kaydedildi. Risk ve borsa sonucu ayrıca doğrulanır."):fresh?`${strategyLabel(row.alpha_family)} · ${value(row.side)}. ${stageLabel(row.state)}. Tetik, veri ve risk koşulları izleniyor.`:evaluation?`${strategyLabel(evaluation.strategy)} · ${value(evaluation.side)}. ${(evaluation.reasons||[]).map(reasonText).join(" ")}`:"Bu parite için güncel strateji değerlendirmesi yok. Analiz motorundaki parite tablosundan canlı taranan pariteleri görebilirsin.";
  $("decision-details").textContent=row||evaluation?JSON.stringify(row||evaluation,null,2):"Bu parite için doğrulanmış karar kaydı yok.";
}

const explanations={
  FUNDING_STRESS:"Paritenin funding maliyeti risk sınırını aşıyor",RETURN_SHOCK:"Paritede ani fiyat şoku var",
  DEMO_ENTRIES_DISABLED:"Testnet giriş izni kapalı",RISK_KILL_SWITCH:"Risk kilidi açık",PRIVATE_STREAM_NOT_READY:"Özel hesap akışı henüz hazır değil",ACCOUNT_RECONCILIATION_REQUIRED:"Hesap kayıtları uzlaştırılmalı",ACCOUNT_RECONCILIATION_STALE:"Güncel hesap uzlaştırması bekleniyor",ACCOUNT_RECONCILIATION_UNAVAILABLE:"Hesap uzlaştırması tamamlanamadı",UNKNOWN_PROTECTIVE_ORDER:"Kaydı eşleşmeyen bir Stop / TP emri var",UNKNOWN_EXCHANGE_ORDER:"Kaydı eşleşmeyen bir borsa emri var",PUBLIC_MARKET_RECOVERY_REQUIRED:"Güncel piyasa verisi bekleniyor",BROAD_ANALYSIS_NOT_READY:"Piyasa genelinde analiz hazırlığı bekleniyor",STORAGE_ADMISSION_HALTED:"Güncel depolama kabulü bekleniyor",CANONICAL_STORAGE_UNAVAILABLE:"Piyasa verisi kaydedilemedi; depolama toparlanması bekleniyor",DISPATCH_STALE:"Emir motorundan güncel ölçüm alınamadı",DISPATCH_NOT_OBSERVED:"Emir motorunun ilk ölçümü bekleniyor",QUANT_TICK_STALE:"Analiz motorunun ölçümü güncel değil",REGIME_UNKNOWN:"Piyasa rejimi henüz belirlenemedi",REGIME_TRANSITION:"Piyasa rejimi geçişte; teyit bekleniyor",WAITING_CLOSED_BAR:"Son analiz korunuyor; yeni kapanan mum bekleniyor",ANALYSIS_STALE:"Analiz kaynağı güncel değil",INSUFFICIENT_HISTORY:"Yeterli kapanmış mum yok",REGIME_NOT_ALLOWED:"Piyasa rejimi bu stratejiye uygun değil",DATA_UNAVAILABLE:"Gerekli veri kalitesi sağlanmadı",STALE_BARS:"Mum verisi güncel değil",STALE_REGIME:"Rejim analizi güncel değil",UNIVERSE_UNAVAILABLE:"İşlem evreni doğrulanamadı",FEATURE_UNAVAILABLE:"Analiz özellikleri eksik",RELATIVE_STRENGTH_UNKNOWN:"BTC’ye göre güç için geçmiş yetersiz",TREND_MISALIGNED:"Üst zaman dilimleri yönü teyit etmiyor",RELATIVE_STRENGTH_OPPOSED:"Göreceli güç işlem yönünü desteklemiyor",STRONG_TREND_FADE:"Güçlü trende karşı dönüş işlemi elendi",SPREAD_GATE:"Alış ve satış farkı yüksek",LIQUIDITY_GATE:"Yakın fiyatlardaki likidite yetersiz",BOOK_UNAVAILABLE:"Emir defteri henüz hazır değil",MARKET_UNAVAILABLE:"Güncel fiyat kanıtı yok",FLOW_UNAVAILABLE:"İşlem akışı henüz hazır değil",FLOW_OPPOSED:"Agresif işlem akışı yönü desteklemiyor",FLOW_NOT_STABILIZED:"İşlem akışındaki dönüş teyit edilmedi",MICROSTRUCTURE_DESYNC:"Fiyat, defter ve işlem akışı eşzamanlı değil",EXPIRED:"Kurulumun süresi doldu",BREAKOUT_INVALIDATED:"Kırılım geçerliliğini kaybetti",VOLATILITY_NOT_EXPANDING:"Kırılımda volatilite genişlemedi",WEAK_PARTICIPATION:"Kırılım hacmi yeterli değil",TRIGGER_ALREADY_PASSED:"Giriş tetiği geride kaldı",PULLBACK_TOO_DEEP:"Geri çekilme fazla derin",STOP_ALREADY_BREACHED:"Stop seviyesi aşıldı",DUPLICATE_SETUP:"Aynı kurulum zaten değerlendirildi",SETUP_NOT_ARMED:"Kurulum henüz tetik bekleme aşamasında değil",NO_SWEEP_RECLAIM:"Süpürme ve geri kazanım oluşmadı",STRETCH_GATE:"Fiyatın banttan uzaklığı uygun değil",WEAK_WICK_REJECTION:"Fitil reddi zayıf",RECLAIM_INVALIDATED:"Geri kazanım bozuldu",INVALID_TARGET_GEOMETRY:"Giriş, stop ve hedef dengesi uygun değil",NO_ARMED_CANDIDATES:"Tetik bekleyen uygun kurulum yok",INSUFFICIENT_STATISTICAL_EVIDENCE:"İstatistiksel kanıt yetersiz",TRIGGER_NOT_CROSSED:"Giriş fiyatının tetiklenmesi bekleniyor",TESTNET_TRIAL_BUDGET_EXHAUSTED:"Testnet deneme bütçesi tamamlandı",TESTNET_TRIAL_SETTLEMENT_REQUIRED:"Önceki denemenin kapanışı bekleniyor",TESTNET_TRIAL_LOSS_LIMIT:"Testnet kayıp bütçesine ulaşıldı",RISK_VERSION_CONFLICT:"Risk durumu değişti; güncel ölçümle yeniden deneyin",EXECUTION_NOT_READY:"Hesap koruması ve özel akış hazır olmalı",PAUSE_REQUIRED:"Önce yeni girişleri durdurun",TESTNET_EXECUTION_REQUIRED:"Testnet otomatik yürütmesi gerekli",COMMAND_EXPIRED:"Komutun süresi doldu",READY:"Emir ön koşulları hazır",ANALYZED:"Güncel analiz tamamlandı"
};
function reasonText(code){return {FEATURE_BAR_UNAVAILABLE:"Analiz özelliğine ait kapanmış mum bulunamadı",BAR_RECOVERY_REQUIRED:"Mum geçmişinin yeniden hazırlanması gerekiyor",ALPHA_INPUT_INVALID:"Strateji girdisi doğrulanamadı",PREVIOUS_FLOW_UNAVAILABLE:"Önceki işlem akışı henüz hazır değil",FUTURE_INPUT:"Girdi zamanı analiz zamanından ileride",CANONICAL_DATA_NOT_READY:"Doğrulanmış piyasa verisi henüz hazır değil",STRESS_DATA_MISSING:"Geniş piyasanın spread ve funding verileri tamamlanmalı",LOW_COVERAGE:"Güncel piyasa kapsamı yetersiz"}[code]||explanations[code]||reasons[code]||"Ayrıntı: "+value(code);}
function stageLabel(state){return {DISCOVERED:"ADAY",WATCHING:"İZLENİYOR",ARMED:"TETİK BEKLİYOR",VALIDATING:"RİSK KONTROLÜ",APPROVED:"ONAYLANDI",EXECUTING:"BORSAYA GÖNDERİLİYOR",SETTLED:"TAMAMLANDI",REJECTED:"ELENDİ",EXPIRED:"SÜRESİ DOLDU",NO_TRADE:"NO_TRADE"}[state]||value(state);}
function strategyLabel(name){return {MOMENTUM:"Momentum · kırılım / geri çekilme",REVERSAL:"Dönüş · süpürme / geri kazanım"}[name]||value(name);}
function strategyShortLabel(name){return {MOMENTUM:"Momentum",REVERSAL:"Dönüş"}[name]||value(name);}
function overviewStrategies(row,label){return row.strategy?label(row.strategy):(row.strategies||[]).map(label).join(" / ");}
function localTime(at){const stamp=Date.parse(at);return Number.isFinite(stamp)?new Date(stamp).toLocaleTimeString("tr-TR",{timeZone:"Europe/Istanbul",hour:"2-digit",minute:"2-digit",second:"2-digit"})+" TRT":"—";}

function renderOverview(data){
  overviewData=data;const live=data.live_analysis||null,engine=live?.engine||{},pairs=live?.pairs||{},sections=data.sections||{};
  const workers=sections.workers?.data||[],quant=workers.find(row=>row.service==="quant_core"),market=workers.find(row=>row.service==="market_gateway"),execution=data.execution_readiness||{};
  overviewLive=backendConnected&&data.status!=="STALE";
  const quantReady=overviewLive&&quant?.freshness==="OK",marketReady=overviewLive&&market?.freshness==="OK"&&market.data_ready===true,analysisReady=quantReady&&data.analysis_readiness?.ready===true;
  const orderReady=overviewLive&&execution.ready===true,entriesAllowed=orderReady&&execution.entries_enabled===true;
  const candidates=overviewLive?(sections.candidates?.data||[]).filter(row=>row.freshness==="OK"):[];
  const armed=candidates.some(row=>row.state==="ARMED")||(quantReady&&Number(quant.evaluated_count)>0&&(quant.symbol_results||[]).some(row=>row.state==="ARMED"));
  const regime=engine.regime||quant?.regime_label;
  const strategyAllowed=["TREND_EXPANSION","RANGE"].includes(regime);
  ovText("runtime-mode",entriesAllowed?"TESTNET":overviewLive?"GÖZLEM":"Doğrulanamadı");
  ovBadge("overview-connection",overviewLive?"● WebSocket · bağlı":"● Son veri · bağlantı bekleniyor",overviewLive?"OK":"UNKNOWN");
  ovText("overview-updated",overviewLive?"Ölçüm: "+localTime(data.evaluated_at||data.as_of):"Canlı ölçüm doğrulanamadı");
  ovBadge("algorithm-status",!overviewLive?"● Doğrulanamadı":quantReady?"● Analiz motoru çalışıyor":"● Analiz bekliyor",quantReady?"ACTIVE":"UNKNOWN");
  ovText("algorithm-headline",!overviewLive?"Piyasa bağlantısı doğrulanıyor":engine.state==="ERROR"?"Analiz motorunda hata var":engine.state==="DELAYED"?"Analiz verisi gecikiyor":quant?.regime_label==="DISORDER"?"Stratejiler bu piyasa rejimine uygun değil":orderReady&&analysisReady&&armed&&strategyAllowed?"Koşullar hazır. Tetik izleniyor.":entriesAllowed&&analysisReady?"Piyasa izleniyor, işlem koşulları hazır":analysisReady?"Piyasa analizi güncel, fırsatlar izleniyor":quantReady?"Tarama devam ediyor, teyit bekleniyor":"Güncel analiz ölçümü bekleniyor");
  ovText("algorithm-explanation",overviewLive?[ovReason(engine.reason||quant?.reason),engine.scan_source_at?"Tarama mumu: "+localTime(engine.scan_source_at):null].filter(Boolean).join(" · "):"Bağlantı kesildiğinde son ölçümler korunur, ancak canlı durum ve işlem hazırlığı doğrulanamaz.");
  ovText("data-status",marketReady?"Güncel":overviewLive?"Doğrulanmadı":"Bilinmiyor");ovText("analysis-status",analysisReady?"Hazır":quantReady?"Teyit bekliyor":"Bilinmiyor");ovText("execution-status",orderReady?"Emir koşulları hazır":overviewLive?"Girişler bekliyor":"Bilinmiyor");
  for(const [id,yes] of [["data-status",marketReady],["analysis-status",analysisReady],["execution-status",orderReady]])$(id).className=yes?"ready":"";
  ovText("execution-note",entriesAllowed?"Sunucu, yeni testnet girişlerini yetkilendiriyor; risk kontrolleri her emir için ayrıca çalışır.":overviewLive?"Gözlem modu · yeni emir için risk, veri ve hesap doğrulaması gerekli.":"Emir hazırlığı bilinmiyor; bağlantının geri gelmesi bekleniyor.");
  ovText("regime-label",quantReady?regimeText(engine.regime||quant.regime_label):"—");ovText("engine-timeframe",overviewLive?engine.timeframe||"—":"—");
  ovText("success-time",localTime(quant?.last_success_at));ovText("analysis-time",quantReady?localTime(engine.last_analysis_at||quant?.last_analysis_at):"—");ovText("analysis-source-time",quantReady?localTime(engine.last_source_at||quant?.last_analysis_source_at):"—");ovText("evaluation-time",localTime(engine.last_evaluation_at||quant?.last_evaluation_at));ovText("evaluated-count",quantReady?ovNum(pairs.evaluated??quant?.evaluated_count)+" yön / strateji":"—");
  ovText("engine-pair-total",overviewLive&&sections.universe?.status==="OK"?ovNum(pairs.eligible):"—");
  ovText("engine-pair-scan",overviewLive?ovNum(pairs.scanned):"—");ovText("engine-pair-analyzed",overviewLive?ovNum(pairs.analyzed):"—");ovText("engine-evaluated",quantReady?ovNum(pairs.evaluated??quant?.evaluated_count):"—");
  const streams=sections.streams?.data?.[0],streamSafe=overviewLive&&["OK","DEGRADED"].includes(sections.streams?.status)&&streams?.clock_safe===true;
  ovText("ov-coverage",streamSafe&&Number(pairs.feed_total)>0&&pairs.feed!=null?Math.round(pairs.feed/pairs.feed_total*100)+"%":"—");
  ovText("ov-lag",overviewLive?ovElapsed(live?.latency?.analysis_seconds):"—");
  ovBadge("engine-state",overviewLive?overviewEngineText[engine.state]||"Doğrulanamadı":"Veri bekleniyor",overviewLive?engine.state:"UNKNOWN");
  ovText("engine-updated",overviewLive?"Güncelleme: "+localTime(data.evaluated_at||data.as_of):"Son ölçüm güncel değil");
  ovText("engine-source",overviewLive?[engine.timeframe||"—",ovNum(pairs.scanned)+" tarama",regimeText(engine.regime)].join(" · "):"Tarama kaynağı bekleniyor");
  // Do not infer full deep coverage from a market feed connection or a scan result.
  overviewRows=Array.isArray(live?.pairs_detail)?live.pairs_detail.filter(row=>row&&typeof row.symbol==="string"):[];
  if(!overviewRows.find(row=>row.symbol===overviewSelected))overviewSelected=overviewRows.find(row=>row.stage==="DEEP")?.symbol||overviewRows[0]?.symbol||"";
  ovText("engine-pair-note",overviewRows.length?`${overviewRows.length} paritenin son analiz kaydı · ${ovNum(pairs.analyzed)} derin analiz${overviewLive?"":" · güncel değil"}`:overviewLive?"Motorun ilk tarama sonucu bekleniyor.":"Son piyasa analizi doğrulanamıyor.");
  renderEnginePairs();
  $("pipeline").replaceChildren();
  const phases=[["01","Evren","Borsada uygun USDT sürekli vadeli pariteler",pairs.eligible],["02","Piyasa taraması","Kapanmış mum, güç, trend ve volatilite",pairs.scanned],["03","Derin inceleme","Emir defteri ve akış · Seçili derin "+ovNum(pairs.selected_deep??quant?.selected_deep_count)+" · Hazır derin "+ovNum(pairs.ready_deep??quant?.ready_deep_count),pairs.analyzed??quant?.analyzed_symbols],["04","Strateji ve risk","Sinyal, risk, emir ve borsa teyidi",pairs.evaluated]];
  for(const [index,title,description,count] of phases){const item=node("div","","ov-route-item");item.append(node("small",index),node("strong",title),node("span",overviewLive?ovNum(count):"—"),node("p",description));$("pipeline").append(item);}
  $("engine-pipeline").replaceChildren();for(const stage of (live?.pipeline||[])){const item=node("div","","ov-pipeline-state "+(overviewLive?ovClass(stage.state):"unknown"));item.append(node("span",overviewPipelineText[stage.stage]||value(stage.stage)),node("strong",overviewLive?overviewPipelineState[stage.state]||value(stage.state):"Bilinmiyor"));$("engine-pipeline").append(item);}
  const rate=overviewRate(data);ovText("engine-market-rate",ovRate(rate.market));ovText("engine-analysis-rate",ovRate(rate.evaluation));
  ovText("engine-last-signal",overviewLive&&live?.last_signal_at?localTime(live.last_signal_at)+(live.last_signal_fresh?"":" · eski"):"—");
  ovText("engine-latency",overviewLive?"WS "+ovElapsed(live?.latency?.market_seconds)+" · tarama "+ovElapsed(live?.latency?.scan_seconds):"—");
  const links=live?.connections||{};ovBadge("ov-health-indicator",overviewLive&&links.binance_websocket==="CONNECTED"?"WS bağlı":overviewLive?"Akış doğrulanıyor":"Bağlı değil",overviewLive?links.binance_websocket:"UNKNOWN");
  const connectionRoot=$("engine-connections");connectionRoot.replaceChildren();
  for(const [name,state,detail] of [["Panel bağlantısı",overviewLive?"CONNECTED":"UNKNOWN",overviewLive?"WebSocket açık":"Yeni ölçüm bekleniyor"],["Binance WebSocket",overviewLive?links.binance_websocket:"UNKNOWN",links.ws_reconnects==null?null:links.ws_reconnects+" yeniden bağlanma"],["Binance REST",overviewLive?links.binance_rest:"UNKNOWN",links.rest_last_success_at?"Son başarılı "+localTime(links.rest_last_success_at):links.rest_last_reason]]){
    const row=node("div","","ov-connection");const label=node("span",name),valueNode=node("div","","ov-connection-right");valueNode.append(node("strong",({CONNECTED:"Bağlı",NO_DATA:"Veri yok",UNAVAILABLE:"Kullanılamıyor",RATE_LIMITED:"İstek sınırı",STALE:"Güncel değil",NOT_SAMPLED:"Ölçülmedi",UNKNOWN:"Bilinmiyor"})[state]||"Bilinmiyor","ov-"+ovClass(state)));if(detail&&overviewLive)valueNode.append(node("small",String(detail)));row.append(label,valueNode);connectionRoot.append(row);
  }
  const blockers=$("engine-blockers");blockers.replaceChildren();const codes=overviewLive?[...new Set([...(execution.blockers||[]),...(!analysisReady&&engine.reason?[engine.reason]:[])].filter(Boolean))]:[];
  for(const code of codes.slice(0,5)){const row=node("div","","ov-reason");row.append(node("span","!"),node("p",ovReason(code)));blockers.append(row);}if(!codes.length)blockers.append(node("p",overviewLive?"Şu anda bildirilen kritik motor engeli yok; bu işlem onayı değildir.":"Engel listesi doğrulanamadı; bağlantı bekleniyor.","ov-muted"));
  const reasonRoot=$("engine-rejection-reasons");reasonRoot.replaceChildren();const counts=Object.entries(quant?.last_evaluation?.reason_counts||quant?.reason_counts||{}).filter(([,v])=>Number.isFinite(Number(v))).sort((a,b)=>Number(b[1])-Number(a[1])).slice(0,5);const max=Math.max(1,...counts.map(([,c])=>Number(c)));
  for(const [code,count] of counts){const row=node("div","","ov-reason-meter"),info=node("div","","ov-reason-meter-top");info.append(node("span",ovReason(code)),node("strong",String(count)));const track=document.createElement("progress");track.max=max;track.value=Number(count);row.append(info,track);reasonRoot.append(row);}
  const input=quant?.last_input_check?.input_rejections;ovText("engine-input-rejections-note",Array.isArray(input)?"Son girdi kontrolü · "+localTime(quant.last_input_check.as_of):"Girdi kontrolü ölçülmedi");
  table("engine-input-rejections",Array.isArray(input)?input.map(row=>({symbol:row.symbol,reason:ovReason(row.reason)})):[],[["symbol","PARİTE"],["reason","RED NEDENİ"]],Array.isArray(input)?"Son ölçümde girdi reddi bulunmuyor.":"Henüz doğrulanmış girdi reddi ölçümü yok.");
  $("engine-errors").replaceChildren();for(const err of (live?.errors||[]).slice(0,8)){const item=node("div","","ov-reason");item.append(node("span","!"),node("p",[ovReason(err.code),err.symbol,err.at?localTime(err.at):null].filter(Boolean).join(" · ")));$("engine-errors").append(item);}if(!(live?.errors||[]).length)$("engine-errors").append(node("p",overviewLive?"Kaydedilmiş motor hatası bulunmuyor.":"Hata durumu doğrulanamıyor.","ov-muted"));
  const events=$("ov-events");events.replaceChildren();const signals=(sections.signals?.data||[]).filter(row=>row&&row.symbol).slice(0,6);
  for(const signal of signals){const line=node("div","","ov-event");const marker=node("span","","ov-event-dot "+ovClass(signal.side));const body=node("div");body.append(node("strong",signal.symbol+" · "+stageLabel(signal.state)),node("small",[strategyLabel(signal.alpha_family),localTime(signal.as_of),overviewLive?null:"Eski kayıt"].filter(Boolean).join(" · ")));line.append(marker,body);events.append(line);}
  if(!signals.length)events.append(node("p",overviewLive?"Son ölçümde doğrulanmış sinyal kaydı yok. Tarama sonuçları yukarıda izlenebilir.":"Canlı sinyal akışı bekleniyor.","ov-placeholder"));
}

function api(path,current=()=>true,lane=path,timeout=12000){
  if(!dbLanes.has(lane))return requestApi(path,current,lane,timeout,true);
  return new Promise((resolve,reject)=>{
    // One pending request per lane; replacement keeps its FIFO position.
    pendingReads.get(lane)?.reject(new Error("REQUEST_SUPERSEDED"));
    pendingReads.set(lane,{path,current,lane,timeout,resolve,reject});
    drainReads();
  });
}
async function drainReads(){
  if(drainingReads)return;
  drainingReads=true;
  try{
    while(pendingReads.size){
      const lane=pendingReads.keys().next().value,read=pendingReads.get(lane);
      pendingReads.delete(lane);
      try{
        if(!read.current())throw new Error("REQUEST_SUPERSEDED");
        // The server DB thread keeps its admission lock after a socket timeout.
        // Fence obsolete active responses, then wait for the bounded request to
        // settle before admitting another DB read. RAM quotes bypass this queue.
        read.resolve(await requestApi(read.path,read.current,read.lane,read.timeout,false));
      }catch(error){read.reject(error);}
    }
  }finally{drainingReads=false;}
}
async function requestApi(path,current,lane,timeout,abortable){
  if(abortable)requests.get(lane)?.abort();
  const controller=new AbortController();if(abortable)requests.set(lane,controller);
  try{
    if(!current())throw new Error("REQUEST_SUPERSEDED");
    const data=await transport.request(path,{timeout,signal:controller.signal});
    if(!current()||controller.signal.aborted)throw new Error("REQUEST_SUPERSEDED");
    return data;
  }finally{if(requests.get(lane)===controller)requests.delete(lane);}
}
function remember(row){
  const previous=visited.get(row.symbol);
  // A delayed metadata response cannot roll back a newer quote.
  if(previous?.source_at&&(!row.source_at||Date.parse(row.source_at)<Date.parse(previous.source_at)))row={...row,...previous};
  if(visited.size>=256&&!visited.has(row.symbol)){
    const victim=[...visited.keys()].find(name=>name!==selected&&!favorites.has(name));if(victim)visited.delete(victim);
  }
  visited.set(row.symbol,row);return row;
}
function quoteMembers(){return [...new Set([selected,...favorites,...symbols.map(row=>row.symbol)].filter(Boolean))].slice(0,128);}
function fenceQuotes(){quoteGeneration++;requests.get("quotes")?.abort();}
function loadQuotes(){
  if(quotePending)return quotePending;
  const members=quoteMembers();if(!members.length)return Promise.resolve();
  const epoch=quoteGeneration,signature=members.join(",");
  quotePending=(async()=>{
    try{
      const data=await api("/api/market/quotes?symbols="+encodeURIComponent(signature),()=>epoch===quoteGeneration,"quotes",1000);
      if(epoch!==quoteGeneration)return;
      quoteTransport=data.transport||"OK";
      const returned=new Set();
      (data.items||[]).slice(0,128).forEach(row=>{if(members.includes(row.symbol)){returned.add(row.symbol);remember(row);}});
      members.filter(member=>!returned.has(member)).forEach(member=>{const row=visited.get(member);if(row)row.freshness="STALE";});
      renderSymbols();
    }catch{if(epoch===quoteGeneration){quoteTransport="UNAVAILABLE";renderSymbols();}}
    finally{quotePending=null;if(epoch!==quoteGeneration&&transport?.ready)return loadQuotes();}
  })();return quotePending;
}
async function loadSymbols(){
  const epoch=++generation,query=$("search").value.trim().toUpperCase(),page=offset;
  symbolStatus="loading";renderSymbols();
  try{
    const data=await api("/api/market/symbols?search="+encodeURIComponent(query)+"&offset="+page,()=>epoch===generation&&query===$("search").value.trim().toUpperCase()&&page===offset,"symbols");
    if(epoch!==generation)return;
    if(!Array.isArray(data.items))throw new Error("SYMBOL_RESPONSE_INVALID");
    showSymbols(data);
  }catch{if(epoch!==generation)return;symbolStatus="error";renderSymbols();}
}
function showSymbols(data){
  if(!Array.isArray(data.items))return;
  const previous=quoteMembers().join(",");
  symbols=data.items.slice(0,50).map(remember);symbolStatus="ready";$("next").disabled=!data.has_more;$("previous").disabled=offset===0;$("page-number").textContent=String(offset/50+1);renderSymbols();if(!selected&&symbols.length)selectSymbol(symbols[0].symbol);else if(previous!==quoteMembers().join(",")){fenceQuotes();loadQuotes();}
}
function quoteFresh(quote){const age=Date.now()-Date.parse(quote?.source_at);return quoteTransport==="OK"&&quote?.freshness!=="STALE"&&Number.isFinite(age)&&age>=0&&age<15000;}
function quoteText(quote,key="price"){const text=money(quote?.[key]);return text!=="—"&&!quoteFresh(quote)?text+" · eski":text;}
function renderQuote(){const quote=visited.get(selected);$("selected-price").textContent=quoteText(quote);$("selected-volume").textContent=quoteText(quote,"volume");$("selected-price").title=quote?.source_at?"Kaynak: "+quote.source_at+" · "+(quoteFresh(quote)?"Güncel":"Son doğrulanmış değer · eski"):"Kaynak bekleniyor";}
function renderSymbols(){renderQuote();const list=filter==="favorites"?[...visited.values()].filter(r=>favorites.has(r.symbol)&&r.symbol.startsWith($("search").value.trim().toUpperCase())):symbols.map(row=>visited.get(row.symbol)||row);$("symbols").replaceChildren();if(filter!=="favorites"&&symbolStatus!=="ready")$("symbols").append(node("p",symbolStatus==="error"?"Sembol listesine ulaşılamadı. Bağlantı yeniden denetlenecek.":"Sembol listesi yükleniyor…","empty"));list.forEach(row=>{const r=node("div","","symbol-row"+(row.symbol===selected?" active":"")),star=node("button",favorites.has(row.symbol)?"★":"☆","favorite"+(favorites.has(row.symbol)?" on":"")),select=node("button","","symbol-select");star.setAttribute("aria-label",row.symbol+" favori");star.setAttribute("aria-pressed",String(favorites.has(row.symbol)));star.addEventListener("click",()=>{if(favorites.has(row.symbol))favorites.delete(row.symbol);else if(favorites.size<64)favorites.add(row.symbol);fenceQuotes();renderSymbols();loadQuotes();});const label=node("span",row.symbol,"symbol-main");label.append(node("small","PERPETUAL"));select.append(label);select.addEventListener("click",()=>selectSymbol(row.symbol));r.append(star,select,node("span",quoteText(row),"symbol-price"));$("symbols").append(r);});if(!list.length&&(filter==="favorites"||symbolStatus==="ready"))$("symbols").append(node("p",filter==="favorites"?"Favoriler bu oturumda tutulur. Bir sembolü ★ ile ekleyin.":"Eşleşen sembol yok.","empty"));}
async function selectSymbol(member){if(member!==selected)clearChart();selected=member;fenceQuotes();$("selected-symbol").textContent=member;renderSymbols();renderDecision();loadQuotes();await loadChart();}
async function loadChart(){if(!selected)return;const epoch=++chartGeneration,member=selected,tf=timeframe;clearChartMetrics();try{const data=await api("/api/market/chart?symbol="+encodeURIComponent(member)+"&timeframe="+tf,()=>epoch===chartGeneration&&member===selected&&tf===timeframe,"chart");if(epoch!==chartGeneration)return;showChart(data);}catch{if(epoch!==chartGeneration)return;chartFailure();}}
function showChart(data){chartHasData=Boolean(data.bars?.length);chart.set(data);$("chart-status").textContent=data.bars?.length?data.bars.length+" kapanmış mum":"Geçmiş veri bekleniyor";$("chart-time").textContent=data.bars?.length?new Date(data.bars.at(-1).time*1000).toISOString().slice(5,16).replace("T"," ")+" UTC":"—";chartMetricsAt=Date.parse(data.metrics?.as_of)||0;const age=Date.now()-chartMetricsAt,fresh=chartMetricsAt&&age>=0&&age<1800000;$("selected-volatility").textContent=fresh?value(data.metrics.volatility):"—";$("selected-strength").textContent=fresh?value(data.metrics.relative_strength):"—";}
function chartFailure(){clearChartMetrics();$("chart-status").textContent=chartHasData?"Grafik güncellenemedi; son doğrulanmış mumlar gösteriliyor.":"Grafik verisi alınamadı";}
document.querySelectorAll("[data-screen]").forEach(b=>{b.setAttribute("aria-label",{overview:"Genel bakış",terminal:"Piyasa terminali",health:"Sistem sağlığı",binance:"Binance API Ayarları"}[b.dataset.screen]);b.addEventListener("click",()=>screen(b.dataset.screen));});
document.querySelectorAll("[data-timeframe]").forEach(b=>b.addEventListener("click",()=>{timeframe=b.dataset.timeframe;document.querySelectorAll("[data-timeframe]").forEach(x=>x.classList.toggle("selected",x===b));clearChart();loadChart();}));
document.querySelectorAll("[data-filter]").forEach(b=>b.addEventListener("click",()=>{filter=b.dataset.filter;document.querySelectorAll("[data-filter]").forEach(x=>x.classList.toggle("selected",x===b));renderSymbols();}));
$("engine-pair-search").addEventListener("input",()=>{engineSearch=$("engine-pair-search").value;overviewPage=0;renderEnginePairs();});
document.querySelectorAll("[data-engine-filter]").forEach(b=>b.addEventListener("click",()=>{engineFilter=b.dataset.engineFilter;overviewPage=0;document.querySelectorAll("[data-engine-filter]").forEach(x=>{x.classList.toggle("selected",x===b);x.setAttribute("aria-pressed",String(x===b));});renderEnginePairs();}));
$("ov-prev").addEventListener("click",()=>{overviewPage=Math.max(0,overviewPage-1);renderEnginePairs();});
$("ov-next").addEventListener("click",()=>{overviewPage+=1;renderEnginePairs();});
document.querySelectorAll("[data-tab]").forEach(b=>b.addEventListener("click",()=>{tab=b.dataset.tab;document.querySelectorAll("[data-tab]").forEach(x=>{x.classList.toggle("selected",x===b);x.setAttribute("aria-selected",String(x===b));});if(snapshot)render(snapshot);}));
$("search").addEventListener("input",()=>{clearTimeout(searchTimer);generation++;requests.get("symbols")?.abort();fenceQuotes();symbols=[];symbolStatus="loading";renderSymbols();offset=0;searchTimer=setTimeout(loadSymbols,250);});$("previous").addEventListener("click",()=>{offset=Math.max(0,offset-50);loadSymbols();});$("next").addEventListener("click",()=>{offset+=50;loadSymbols();});
setInterval(()=>{expireChartMetrics();$("clock").textContent=localTime(new Date().toISOString());if(snapshot&&expireSources(snapshot))render(snapshot);if(receivedAt&&performance.now()-receivedAt>45000&&backendConnected)stale("Sunucu ile iletişim yeniden kuruluyor.");renderSymbols();},1000);
document.addEventListener("visibilitychange",()=>{ /* UI visibility cannot change exchange/server readiness. */ });
transport=new DashboardSocket({
  onSession(session){
    socketState="CONNECTED";
    canControl=session.scopes?.includes("control")===true;
    $("session-scope").textContent=canControl?"Operatör oturumu":"Salt okunur oturum";
    loadSymbols();if(selected){loadChart();loadQuotes();}
  },
  onSnapshot(data){backendConnected=true;socketState="CONNECTED";receivedAt=performance.now();render(data);$("latency").textContent="WSS · canlı veri bağlantısı";},
  onQuotes(data){quoteTransport=data.transport||"UNAVAILABLE";const members=new Set(quoteMembers());for(const row of (data.items||[]).slice(0,128))if(members.has(row.symbol))remember(row);renderSymbols();},
  onUpdate({operation,params,data,error}){
    if(operation==="symbols"&&params.search===$("search").value.trim().toUpperCase()&&params.offset===offset){if(error){symbolStatus="error";renderSymbols();}else showSymbols(data);}
    if(operation==="chart"&&params.symbol===selected&&params.timeframe===timeframe){if(error)chartFailure();else showChart(data);}
  },
  onDisconnect(){socketState="DISCONNECTED";overviewRateSample=null;canControl=false;quoteTransport="UNAVAILABLE";symbolStatus="error";for(const request of requests.values())request.abort();stale("Canlı bağlantı kesildi; yeniden senkronizasyon bekleniyor.");renderSymbols();}
});
transport.connect();
