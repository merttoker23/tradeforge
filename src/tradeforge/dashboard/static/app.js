"use strict";
const $ = id => document.getElementById(id);
let snapshot=null, generation=0, receivedAt=0, backendConnected=false;
let selected="", timeframe="15m", symbols=[], offset=0, filter="all", tab="positions", searchTimer=null, chartGeneration=0;
let chartMetricsAt=0, chartHasData=false, symbolStatus="loading";
let canControl=false, transport=null;
const requests=new Map(), pendingReads=new Map(), dbLanes=new Set(["snapshot","symbols","chart"]);
let drainingReads=false;
let quotePending=null,quoteGeneration=0,quoteTransport="UNAVAILABLE";
const favorites=new Set(), visited=new Map();
const chart=new TerminalChart($("candles"),$("chart-empty"));
const labels={OK:"Güncel",UNKNOWN:"Veri bekleniyor",STALE:"Güncelliği doğrulanamadı",DEGRADED:"Kısmi veri",NOT_READY:"Hazır değil",FAIL_CLOSED:"Girişler durduruldu",NEW:"Bekliyor",FILLED:"Gerçekleşti",CANCELED:"İptal",LONG:"LONG",SHORT:"SHORT",NO_TRADE:"NO_TRADE",PARTIALLY_FILLED:"Kısmi gerçekleşme"};
const reasons={API_KEY_INVALID:"Demo anahtarı kabul edilmedi. Sunucunun Demo kimlik dosyalarını denetleyin.",API_KEY_PERMISSION_OR_IP_DENIED:"Demo anahtarının izinlerini veya IP kısıtını denetleyin.",DEMO_CREDENTIALS_MISSING:"Demo kimlik dosyalarını sunucuda yapılandırın.",DEMO_SECRET_FILE_UNSAFE:"Sunucunun özel Demo kimlik dosyaları doğrulanamadı.",EXCHANGE_AUTH_REJECTED:"Demo anahtarı veya izinleri kabul edilmedi. Yeni Demo anahtarlarını denetleyin.",EXCHANGE_TRANSPORT_UNKNOWN:"Binance Demo’ya ulaşılamadı. Bağlantı yeniden denetlenecek.",CLOCK_UNSAFE:"Sunucunun saati Binance Demo ile doğrulanamadı. Sistem Sağlığı ayrıntılarını denetleyin.",CLOCK_PROBE_SLOW:"Binance Demo saat kontrolü gecikti. Bağlantı yeniden denetlenecek.",RATE_LIMITED:"Borsa istek sınırına ulaşıldı; bağlantı beklemede.",DEMO_RESPONSE_INVALID:"Demo hesap yanıtı doğrulanamadı.",KEY_PERMISSIONS_UNVERIFIED:"Anahtar yetkileri API üzerinden tam doğrulanamadı.",GUARDIAN_NOT_RUNNING:"Emir koruyucusu başlatılmadı; emir yetkisi kilitli.",INSUFFICIENT_EVIDENCE:"Karar için yeterli doğrulanmış piyasa kanıtı yok.",BTC_HISTORY_MISSING:"Göreceli güç için yeterli eşzamanlı BTC geçmişi yok."};
function value(x){if(x===null||x===undefined)return "—"; if(Array.isArray(x))return x.length?x.map(value).join(" · "):"—";return labels[x]||String(x);}
function money(x){if(x===null||x===undefined||x===""||!Number.isFinite(Number(x)))return "—";return Number(x).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:8});}
function node(tag,text,className){const e=document.createElement(tag);e.textContent=text;if(className)e.className=className;return e;}
function screen(name){document.querySelectorAll(".screen").forEach(e=>e.hidden=e.id!==name+"-screen");document.querySelectorAll("[data-screen]").forEach(e=>e.classList.toggle("active",e.dataset.screen===name));$("screen-label").textContent={overview:"Genel bakış",terminal:"Piyasa terminali",health:"Sistem sağlığı",binance:"Binance API Ayarları"}[name];if(name==="terminal")chart.draw();}
function table(target,rows,columns,empty="Henüz doğrulanmış kayıt yok."){
  const root=$(target);root.replaceChildren();if(!rows?.length){root.append(node("p",empty,"empty"));return;}
  const t=document.createElement("table"),head=document.createElement("thead"),hr=document.createElement("tr"),body=document.createElement("tbody");
  columns.forEach(([,title])=>{const th=node("th",title);th.scope="col";hr.append(th);});head.append(hr);t.append(head);
  rows.slice(0,100).forEach(row=>{const tr=document.createElement("tr");columns.forEach(([key])=>{const td=node("td",value(row[key]));if(["realized_pnl","unrealized_pnl"].includes(key)&&row[key]!=null)td.className=Number(row[key])>=0?"positive":"negative";tr.append(td);});body.append(tr);});t.append(body);root.append(t);
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
  if(data.sections?.demo?.status==="STALE"&&data.demo_readiness){data.demo_readiness.status="STALE";data.demo_readiness.orders_enabled=false;data.demo_readiness.reason_codes=["DEMO_PREFLIGHT_STALE"];if(data.service_health)data.service_health.execution_guardian="NOT_READY";(data.sections.demo.data||[]).forEach(row=>row.guardian="UNKNOWN");}
  if(expired){Object.keys(data.data_readiness||{}).forEach(name=>data.data_readiness[name]=data.sections?.[name]?.status||"UNKNOWN");(data.sections?.workers?.data||[]).forEach(row=>{if(data.service_health)data.service_health[row.service]=row.freshness;});if(data.sections?.streams?.status==="STALE"||data.sections?.universe?.status!=="OK")(data.sections?.universe?.data||[]).forEach(row=>row.stream_coverage=null);}
  const workers=data.sections?.workers?.data||[];
  if(workers.some(w=>w.service==="execution_guardian"&&w.freshness!=="OK")&&data.execution_readiness)data.execution_readiness={...data.execution_readiness,ready:false,entries_enabled:false,reason:"DISPATCH_STALE",blockers:["DISPATCH_STALE"]};
  if(workers.some(w=>w.service==="quant_core"&&w.freshness!=="OK")&&data.analysis_readiness)data.analysis_readiness={ready:false,reason:"QUANT_TICK_STALE"};
  return expired;
}
function render(data) {
  expireSources(data);snapshot=data;const sections=data.sections||{},demo=sections.demo?.data?.[0],fresh=sections.demo?.status==="OK"&&data.status!=="STALE"&&backendConnected,verified=demo?.connection==="OK"&&demo?.account==="OK"&&demo?.balance==="OK"&&demo?.positions==="OK"&&demo?.equity!=null&&demo?.available_balance!=null,account=verified?demo?.terminal:null;
  $("equity").textContent=verified?money(demo?.equity):"—";$("available").textContent=verified?money(demo?.available_balance):"—";$("daily-pnl").textContent=money(account?.daily_pnl);$("total-pnl").textContent=money(account?.total_pnl);$("position-count").textContent=verified?value(demo?.position_count):"—";$("total-risk").textContent=account?.total_risk!=null?money(account.total_risk):"Ölçülemedi";
  $("account-note").textContent=verified?(fresh?"Binance Demo · güncel":"Son doğrulanmış Demo verisi · güncel değil"):"Demo hesap verisi henüz doğrulanmadı";$("demo-connection").textContent=fresh?"● Demo bağlı":verified?"● Son doğrulanmış veri · eski":"● Demo veri bekleniyor";$("demo-connection").className="connection"+(fresh?" ok":"");
  $("guardian-summary").textContent=demo?.guardian==="RUNNING"&&fresh?"Çalışıyor":"Doğrulanamadı";
  const codes=data.demo_readiness?.reason_codes||[],action=codes.find(c=>reasons[c]&&c!=="GUARDIAN_NOT_RUNNING"&&c!=="KEY_PERMISSIONS_UNVERIFIED");
  $("banner").hidden=!action&&data.status!=="STALE";$("banner").className="banner"+(data.status==="STALE"?" stale":"");$("banner").textContent=data.status==="STALE"?"Sunucu verisi geçici olarak alınamıyor; son doğrulanmış değerler korunuyor.":action?reasons[action]:"";
  table("opportunities",sections.opportunities?.status==="STALE"?[]:sections.opportunities?.data,[["symbol","SEMBOL"],["score","SKOR"],["freshness","VERİ"]],"Yeterli kanıt oluştuğunda fırsatlar burada görünür.");
  const columns={positions:[["symbol","SEMBOL"],["side","YÖN"],["qty","MİKTAR"],["entry_price","GİRİŞ"],["mark_price","DEMO MARK"],["unrealized_pnl","AÇIK PNL"]],orders:[["symbol","SEMBOL"],["side","YÖN"],["kind","TÜR"],["qty","MİKTAR"],["price","FİYAT"],["status","DURUM"]],stops:[["symbol","SEMBOL"],["kind","TÜR"],["qty","MİKTAR"],["stop_price","TETİK"],["status","DURUM"]],history:[["symbol","SEMBOL"],["side","YÖN"],["price","GERÇEKLEŞME"],["qty","MİKTAR"],["realized_pnl","PNL"],["commission","KOMİSYON"],["commission_asset","BİRİM"],["as_of","UTC"]]};
  table("account-records",account?.[tab],columns[tab],fresh?"Bu görünümde doğrulanmış Demo kaydı bulunmuyor.":"Demo bağlantısı doğrulanmadı; hesap verisi gösterilemiyor.");$("demo-record-note").textContent="Binance Futures Demo · "+(fresh?"Kaynak: "+value(demo.as_of):"Güncel hesap kanıtı bekleniyor")+(tab==="history"?" · Son 5 ilgili sembol / en fazla 100 işlem; tam hesap geçmişi değildir.":"");
  renderOverview(data);renderDecision();$("services").replaceChildren();Object.entries(data.service_health||{}).forEach(([name,state])=>{const r=node("div","","health-row");r.append(node("span",name),node("span",value(state)));$("services").append(r);});$("data-health").replaceChildren();Object.entries(data.data_readiness||{}).forEach(([name,state])=>{const r=node("div","","health-row");r.append(node("span",name),node("span",value(state)+" · "+state));$("data-health").append(r);});
  (sections.workers?.data||[]).forEach(worker=>{
    const fresh=backendConnected&&data.status!=="STALE"&&worker.freshness==="OK";
    const states=[["Sağlık","healthy"],["Veri","data_ready"],["Analiz","analysis_ready"],["Emir","execution_ready"]].map(([label,key])=>label+": "+(fresh&&worker[key]===true?"Hazır":worker[key]===false?"Hazır değil":"Doğrulanamadı")).join(" · ");
    const row=node("div","","health-row");row.append(node("span",worker.service),node("span",states+" · "+value(worker.stage)));$("data-health").append(row);
  });
  const marketState=sections.streams?.data?.[0]||{};
  const current=backendConnected&&data.status!=="STALE";
  const market=(sections.workers?.data||[]).find(worker=>worker.service==="market_gateway");
  const streamFresh=current&&["OK","DEGRADED"].includes(sections.streams?.status)&&marketState.clock_safe===true;
  if ($("binance-health")) {
    $("binance-health").replaceChildren();
    const rows={"Aktif kaynak":marketState.market_environment||"Doğrulanamadı",
      "REST":Number(marketState.rest_market_covered)>0?"Veri alınıyor":"Doğrulanamadı",
      "WebSocket":streamFresh&&marketState.covered>0?"Güncel veri var":"Doğrulanamadı",
      "Akış durumu":marketState.clock_safe===true&&market?.freshness==="OK"?"Saat ve worker güncel":"Doğrulanamadı"};
    Object.entries(rows).forEach(([name,state])=>{const r=node("div","","health-row");r.append(node("span",name),node("span",String(state)));$("binance-health").append(r);});
  }
  $("sample-time").textContent="Kaynak: "+value(data.as_of);
}
function renderDecision(){
  const rows=snapshot?.sections?.signals?.data||[],row=rows.find(r=>r.symbol===selected),fresh=row&&row.freshness==="OK"&&backendConnected&&snapshot.status!=="STALE",valid=fresh&&row.state==="APPROVED"&&["LONG","SHORT"].includes(row.side);
  const quant=snapshot?.sections?.workers?.data?.find(w=>w.service==="quant_core"),evaluation=backendConnected&&quant?.freshness==="OK"&&snapshot.status!=="STALE"?quant.symbol_results?.find(r=>r.symbol===selected):null;
  $("decision-symbol").textContent=selected||"SEÇİLİ PARİTE";$("decision").textContent=valid?row.side:fresh?stageLabel(row.state):evaluation?stageLabel(evaluation.state):"NO_TRADE";$("decision").className="decision"+(valid?" "+row.side.toLowerCase():"");
  $("decision-explanation").textContent=valid?(row.execution_authority==="TESTNET_TRIAL"?"Testnet denemesi · UNPROVEN. Strateji istatistiksel olarak doğrulanmadı. Risk ve koruma kapıları zorunludur.":"Güncel algoritma kararı kaydedildi. Risk ve borsa sonucu ayrıca doğrulanır."):fresh?`${strategyLabel(row.alpha_family)} · ${value(row.side)}. ${stageLabel(row.state)}. Tetik, veri ve risk koşulları izleniyor.`:evaluation?`${strategyLabel(evaluation.strategy)} · ${value(evaluation.side)}. ${(evaluation.reasons||[]).map(reasonText).join(" ")}`:"Bu parite için güncel strateji değerlendirmesi yok. Radar listesinden aktif izlenen pariteleri görebilirsin.";
  $("decision-details").textContent=row||evaluation?JSON.stringify(row||evaluation,null,2):"Bu parite için doğrulanmış karar kaydı yok.";
}

const explanations={
  FUNDING_STRESS:"Paritenin funding maliyeti risk sınırını aşıyor",RETURN_SHOCK:"Paritede ani fiyat şoku var",
  DEMO_ENTRIES_DISABLED:"Testnet giriş izni kapalı",RISK_KILL_SWITCH:"Risk kilidi açık",PRIVATE_STREAM_NOT_READY:"Özel hesap akışı henüz hazır değil",ACCOUNT_RECONCILIATION_REQUIRED:"Hesap kayıtları uzlaştırılmalı",ACCOUNT_RECONCILIATION_STALE:"Güncel hesap uzlaştırması bekleniyor",ACCOUNT_RECONCILIATION_UNAVAILABLE:"Hesap uzlaştırması tamamlanamadı",UNKNOWN_PROTECTIVE_ORDER:"Kaydı eşleşmeyen bir Stop / TP emri var",UNKNOWN_EXCHANGE_ORDER:"Kaydı eşleşmeyen bir borsa emri var",PUBLIC_MARKET_RECOVERY_REQUIRED:"Güncel piyasa verisi bekleniyor",BROAD_ANALYSIS_NOT_READY:"Piyasa genelinde analiz hazırlığı bekleniyor",STORAGE_ADMISSION_HALTED:"Güncel depolama kabulü bekleniyor",CANONICAL_STORAGE_UNAVAILABLE:"Piyasa verisi kaydedilemedi; depolama toparlanması bekleniyor",DISPATCH_STALE:"Emir motorundan güncel ölçüm alınamadı",DISPATCH_NOT_OBSERVED:"Emir motorunun ilk ölçümü bekleniyor",QUANT_TICK_STALE:"Analiz döngüsünün ölçümü güncel değil",REGIME_UNKNOWN:"Piyasa rejimi henüz belirlenemedi",REGIME_TRANSITION:"Piyasa rejimi geçişte; teyit bekleniyor",WAITING_CLOSED_BAR:"Son analiz korunuyor; yeni mum kapanışı bekleniyor",WAITING_HISTORY_SWEEP:"Paritelerin geçmiş mumları hazırlanıyor",ANALYSIS_STALE:"Analiz kaynağı güncel değil",INSUFFICIENT_HISTORY:"Yeterli kapanmış mum yok",REGIME_NOT_ALLOWED:"Piyasa rejimi bu stratejiye uygun değil",DATA_UNAVAILABLE:"Gerekli veri kalitesi sağlanmadı",STALE_BARS:"Mum verisi güncel değil",STALE_REGIME:"Rejim analizi güncel değil",UNIVERSE_UNAVAILABLE:"İşlem evreni doğrulanamadı",FEATURE_UNAVAILABLE:"Analiz özellikleri eksik",RELATIVE_STRENGTH_UNKNOWN:"BTC’ye göre güç için geçmiş yetersiz",TREND_MISALIGNED:"Üst zaman dilimleri yönü teyit etmiyor",RELATIVE_STRENGTH_OPPOSED:"Göreceli güç işlem yönünü desteklemiyor",STRONG_TREND_FADE:"Güçlü trende karşı dönüş işlemi elendi",SPREAD_GATE:"Alış ve satış farkı yüksek",LIQUIDITY_GATE:"Yakın fiyatlardaki likidite yetersiz",BOOK_UNAVAILABLE:"Emir defteri henüz hazır değil",MARKET_UNAVAILABLE:"Güncel fiyat kanıtı yok",FLOW_UNAVAILABLE:"İşlem akışı henüz hazır değil",FLOW_OPPOSED:"Agresif işlem akışı yönü desteklemiyor",FLOW_NOT_STABILIZED:"İşlem akışındaki dönüş teyit edilmedi",MICROSTRUCTURE_DESYNC:"Fiyat, defter ve işlem akışı eşzamanlı değil",EXPIRED:"Kurulumun süresi doldu",BREAKOUT_INVALIDATED:"Kırılım geçerliliğini kaybetti",VOLATILITY_NOT_EXPANDING:"Kırılımda volatilite genişlemedi",WEAK_PARTICIPATION:"Kırılım hacmi yeterli değil",TRIGGER_ALREADY_PASSED:"Giriş tetiği geride kaldı",PULLBACK_TOO_DEEP:"Geri çekilme fazla derin",STOP_ALREADY_BREACHED:"Stop seviyesi aşıldı",DUPLICATE_SETUP:"Aynı kurulum zaten değerlendirildi",SETUP_NOT_ARMED:"Kurulum henüz tetik bekleme aşamasında değil",NO_SWEEP_RECLAIM:"Süpürme ve geri kazanım oluşmadı",STRETCH_GATE:"Fiyatın banttan uzaklığı uygun değil",WEAK_WICK_REJECTION:"Fitil reddi zayıf",RECLAIM_INVALIDATED:"Geri kazanım bozuldu",INVALID_TARGET_GEOMETRY:"Giriş, stop ve hedef dengesi uygun değil",NO_ARMED_CANDIDATES:"Tetik bekleyen uygun kurulum yok",INSUFFICIENT_STATISTICAL_EVIDENCE:"İstatistiksel kanıt yetersiz",TRIGGER_NOT_CROSSED:"Giriş fiyatının tetiklenmesi bekleniyor",TESTNET_TRIAL_BUDGET_EXHAUSTED:"Testnet deneme bütçesi tamamlandı",TESTNET_TRIAL_SETTLEMENT_REQUIRED:"Önceki denemenin kapanışı bekleniyor",TESTNET_TRIAL_LOSS_LIMIT:"Testnet kayıp bütçesine ulaşıldı",RISK_VERSION_CONFLICT:"Risk durumu değişti; güncel ölçümle yeniden deneyin",GUARDIAN_NOT_READY:"Hesap koruması ve özel akış hazır olmalı",PAUSE_REQUIRED:"Önce yeni girişleri durdurun",TESTNET_GUARDIAN_REQUIRED:"Testnet emir koruyucusu gerekli",COMMAND_EXPIRED:"Komutun süresi doldu",READY:"Emir ön koşulları hazır",ANALYZED:"Güncel analiz tamamlandı"
};
function reasonText(code){return {FEATURE_BAR_UNAVAILABLE:"Analiz özelliğine ait kapanmış mum bulunamadı",BAR_RECOVERY_REQUIRED:"Mum geçmişinin yeniden hazırlanması gerekiyor",ALPHA_INPUT_INVALID:"Strateji girdisi doğrulanamadı",PREVIOUS_FLOW_UNAVAILABLE:"Önceki işlem akışı henüz hazır değil",FUTURE_INPUT:"Girdi zamanı analiz zamanından ileride",CANONICAL_DATA_NOT_READY:"Doğrulanmış piyasa verisi henüz hazır değil",STRESS_DATA_MISSING:"Geniş piyasanın spread ve funding verileri tamamlanmalı",LOW_COVERAGE:"Güncel piyasa kapsamı yetersiz"}[code]||explanations[code]||reasons[code]||"Ayrıntı: "+value(code);}
function stageLabel(state){return {DISCOVERED:"ADAY",WATCHING:"İZLENİYOR",ARMED:"TETİK BEKLİYOR",VALIDATING:"RİSK KONTROLÜ",APPROVED:"ONAYLANDI",EXECUTING:"BORSAYA GÖNDERİLİYOR",SETTLED:"TAMAMLANDI",REJECTED:"ELENDİ",EXPIRED:"SÜRESİ DOLDU",NO_TRADE:"NO_TRADE"}[state]||value(state);}
function strategyLabel(name){return {MOMENTUM:"Momentum · kırılım / geri çekilme",REVERSAL:"Dönüş · süpürme / geri kazanım"}[name]||value(name);}
function localTime(at){const stamp=Date.parse(at);return Number.isFinite(stamp)?new Date(stamp).toLocaleTimeString("tr-TR",{timeZone:"Europe/Istanbul",hour:"2-digit",minute:"2-digit",second:"2-digit"})+" TRT":"—";}
function renderOverview(data){
  const current=backendConnected&&data.status!=="STALE",sections=data.sections||{},workers=sections.workers?.data||[],quant=workers.find(w=>w.service==="quant_core"),market=workers.find(w=>w.service==="market_gateway"),execution=data.execution_readiness||{};
  const quantFresh=current&&quant?.freshness==="OK",stream=sections.streams?.data?.[0],streamFresh=current&&["OK","DEGRADED"].includes(sections.streams?.status)&&stream?.clock_safe===true,analyzed=current&&data.analysis_readiness?.ready===true,ready=current&&execution.ready===true;
  $("runtime-mode").textContent=current&&execution.entries_enabled===true?"TESTNET":current&&workers.some(w=>w.service==="execution_guardian"&&w.freshness==="OK")?"GÖZLEM":"Doğrulanamadı";
  $("execution-status").textContent=ready?"Emir koşulları hazır":current?"Yeni girişler bekliyor":"Doğrulanamadı";$("execution-status").className=ready?"ready":"";
  $("data-status").textContent=current&&market?.freshness==="OK"&&market.data_ready===true?"Veri geliyor":"Doğrulanamadı";$("data-status").className=current&&market?.freshness==="OK"&&market.data_ready===true?"ready":"";
  $("analysis-status").textContent=analyzed?"Analiz hazır":quantFresh?"Koşullar bekleniyor":"Doğrulanamadı";$("analysis-status").className=analyzed?"ready":"";
  $("algorithm-status").textContent=quantFresh?"● Analiz döngüsü çalışıyor":"● Güncel analiz ölçümü bekleniyor";$("algorithm-status").className="status-pill"+(quantFresh?(analyzed?"":" waiting"):" unknown");
  const strategiesSuitable=quantFresh&&["TREND_EXPANSION","RANGE"].includes(quant.regime_label),candidates=current?(sections.candidates?.data||[]).filter(r=>r.freshness==="OK"):null;
  const armed=candidates?.some(r=>r.state==="ARMED")||(quantFresh&&quant.evaluated_count>0&&(quant.symbol_results||[]).some(r=>r.state==="ARMED"));
  $("algorithm-headline").textContent=quantFresh&&quant.regime_label==="DISORDER"?"Stratejiler bu piyasa rejimine uygun değil.":ready&&analyzed&&strategiesSuitable&&armed?"Koşullar hazır. Tetik izleniyor.":ready?"Emir ön koşulları hazır. Uygun aday bekleniyor.":analyzed?"Piyasa analizi hazır.":quantFresh?"Motor çalışıyor. Teyit bekliyor.":"Algoritmanın durumu doğrulanıyor.";
  $("algorithm-explanation").textContent=quantFresh?[reasonText(quant.reason),...(quant.regime_reasons||[]).filter(c=>["STRESS_DATA_MISSING","LOW_COVERAGE","INSUFFICIENT_HISTORY"].includes(c)).map(reasonText)].join(". ")+". Momentum ve dönüş stratejileri, uygun rejim ve güncel veri olduğunda değerlendirilir.":"Yeni bir analiz ölçümü alınana kadar işlem hazırlığı doğrulanamaz. Son hesap kayıtları kendi kaynak zamanlarıyla korunur.";
  $("execution-note").textContent=current?(execution.trial_enabled?"Sınırlı testnet denemesi · strateji istatistiksel olarak doğrulanmış sayılmaz.":"Gözlem / doğrulanmış istatistiksel kanıt yolu. Emir izni ve risk kilidi ayrı kontrollerdir."):"Sunucu bağlantısı bekleniyor; işlem hazırlığı doğrulanamadı.";
  $("cycle-time").textContent=localTime(quant?.last_success_at);$("analysis-time").textContent=localTime(quant?.last_analysis_at);$("analysis-source-time").textContent=localTime(quant?.last_analysis_source_at);$("evaluation-time").textContent=localTime(quant?.last_evaluation_at);$("regime-label").textContent=quantFresh?({UNKNOWN:"Belirlenemedi",TRANSITION:"Geçiş",TREND_EXPANSION:"Trend genişlemesi",RANGE:"Yatay piyasa",VOL_COMPRESSION:"Volatilite sıkışması",DISORDER:"Düzensiz piyasa"}[quant.regime_label]||value(quant.regime_label)):"—";$("evaluated-count").textContent=quantFresh?value(quant.evaluated_count)+" yön / strateji":"—";$("overview-updated").textContent="Ölçüm: "+localTime(data.as_of);
  const watched=streamFresh&&Array.isArray(stream.watched)?stream.watched:null,deep=streamFresh&&Array.isArray(stream.deep)?stream.deep:null,orders=current?sections.orders?.data:null;
  $("pipeline").replaceChildren();
  [["Uygun parite",current&&sections.universe?.status==="OK"?sections.universe.data?.[0]?.eligible:null,"Keşfedilen evren"],["Güncel stream",streamFresh?stream.covered:null,"Ölçülen kapsam"],["İzlenen",watched?.length,"Aktif abonelik"],["Seçili derin",quantFresh?quant.selected_deep_count:null,"Analiz için seçilen"],["Hazır derin",quantFresh?quant.ready_deep_count:null,"Defter + akış hazır"],["Aktif aday",candidates?.length,"Son 100 karar"],["Gerçekleşen",orders?.filter(r=>r.status==="FILLED").length,"Son 100 yerel emir"]].forEach(([title,count,note])=>{const step=node("div","","pipeline-step");step.append(node("span",title),node("strong",value(count)),node("small",note));$("pipeline").append(step);});
  $("radar").replaceChildren();$("radar-count").textContent=watched?watched.length+" parite":"Ölçüm bekleniyor";
  if(!watched?.length)$("radar").append(node("p",watched?"Bu döngüde izleme grubuna alınan parite yok.":"Aktif izleme listesi henüz doğrulanmadı.","empty"));
  (watched||[]).slice(0,64).sort((a,b)=>Number(deep?.includes(b))-Number(deep?.includes(a))).forEach(symbol=>{const button=node("button",symbol,deep?.includes(symbol)?"deep":"");button.append(node("small",deep?.includes(symbol)?"DERİN":"İZLEME"));button.addEventListener("click",()=>{screen("terminal");selectSymbol(symbol);});$("radar").append(button);});
  const evaluation=quant?.last_evaluation||(quant?.symbol_results?.length?quant:null),evaluations=evaluation?.symbol_results||[];
  $("evaluation-note").textContent=evaluation?"Son tamamlanan değerlendirme · "+localTime(evaluation.as_of||quant?.last_evaluation_at)+(quantFresh?" · Yeni sonuç geldikçe yenilenir":" · Güncel değil; son sonuçlar korunuyor"):"İlk strateji değerlendirmesi bekleniyor.";
  table("analysis-results",evaluations.map(r=>({...r,strategy:strategyLabel(r.strategy),state:stageLabel(r.state),why:(r.reasons||[]).map(reasonText).join(" · ")})),[["symbol","PARİTE"],["strategy","STRATEJİ"],["side","YÖN"],["state","AŞAMA"],["why","GEREKÇE"]],quantFresh?reasonText(quant.reason)+". Henüz strateji değerlendirmesi yok.":"Güncel strateji değerlendirmesi bekleniyor.");
  const codes=current?[...new Set([...(execution.blockers||[]),...(!ready&&execution.reason&&execution.reason!=="READY"?[execution.reason]:[]),...(!analyzed&&quantFresh?[quant.reason]:[])])]:["DISPATCH_STALE"];
  $("blockers").replaceChildren();(codes.length?codes:[ready?"READY":"DISPATCH_NOT_OBSERVED"]).slice(0,8).forEach(code=>{const row=node("div","","blocker"),copy=node("div",reasonText(code));row.append(node("span",code==="READY"?"✓":"·"),copy);$("blockers").append(row);});
  $("rejection-reasons").replaceChildren();const counts=Object.entries(evaluation?.reason_counts||{}).sort((a,b)=>b[1]-a[1]).slice(0,5),maximum=Math.max(1,...counts.map(([,count])=>count));
  if(!counts.length)$("rejection-reasons").append(node("p","Bu döngüde kayıtlı strateji elemesi yok.","muted"));
  counts.forEach(([code,count])=>{const row=node("div","","reason-row"),caption=node("div","","reason-caption"),track=node("progress","","reason-track");caption.append(node("span",reasonText(code)),node("span",String(count)));track.setAttribute("max",String(maximum));track.setAttribute("value",String(count));track.setAttribute("aria-label",reasonText(code));row.append(caption,track);$("rejection-reasons").append(row);});
  const inputCheck=quant?.last_input_check||(Array.isArray(quant?.input_rejections)?quant:evaluation),inputRejections=inputCheck?.input_rejections;
  $("input-rejections-note").textContent=Array.isArray(inputRejections)?"Son girdi kontrolü · "+localTime(inputCheck.as_of)+(quantFresh?"":" · Güncel değil"):"";
  table("input-rejections",inputRejections?.map(row=>({...row,reason:reasonText(row.reason)}))||[],[["symbol","PARİTE"],["reason","GİRDİ HAZIRLIĞI"]],Array.isArray(inputRejections)?"Son ölçümde kayıtlı girdi reddi yok.":"Güncel girdi hazırlığı ölçümü bekleniyor.");
  table("decision-timeline",(sections.signals?.data||[]).map(r=>({...r,alpha_family:strategyLabel(r.alpha_family),state:stageLabel(r.state),freshness:current?r.freshness:"STALE",as_of:localTime(r.as_of),reason:(r.reason_codes||[]).map(reasonText).join(" · ")})),[["symbol","PARİTE"],["alpha_family","STRATEJİ"],["side","YÖN"],["state","SON AŞAMA"],["trigger","TETİK"],["stop_proposal","STOP"],["take_profit_proposal","TP"],["freshness","VERİ"],["as_of","KAYNAK · TRT"],["reason","GEREKÇE"]],"Henüz kalıcı işlem adayı oluşmadı. Tarama durumu ve eleme nedenleri yukarıda görünür.");
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
document.querySelectorAll("[data-tab]").forEach(b=>b.addEventListener("click",()=>{tab=b.dataset.tab;document.querySelectorAll("[data-tab]").forEach(x=>{x.classList.toggle("selected",x===b);x.setAttribute("aria-selected",String(x===b));});if(snapshot)render(snapshot);}));
$("search").addEventListener("input",()=>{clearTimeout(searchTimer);generation++;requests.get("symbols")?.abort();fenceQuotes();symbols=[];symbolStatus="loading";renderSymbols();offset=0;searchTimer=setTimeout(loadSymbols,250);});$("previous").addEventListener("click",()=>{offset=Math.max(0,offset-50);loadSymbols();});$("next").addEventListener("click",()=>{offset+=50;loadSymbols();});
setInterval(()=>{expireChartMetrics();$("clock").textContent=localTime(new Date().toISOString());if(snapshot&&expireSources(snapshot))render(snapshot);if(receivedAt&&performance.now()-receivedAt>45000&&backendConnected)stale("Sunucu ile iletişim yeniden kuruluyor.");renderSymbols();},1000);
document.addEventListener("visibilitychange",()=>{ /* UI visibility cannot change exchange/server readiness. */ });
transport=new DashboardSocket({
  onSession(session){
    canControl=session.scopes?.includes("control")===true;
    $("session-scope").textContent=canControl?"Operatör oturumu":"Salt okunur oturum";
    loadSymbols();if(selected){loadChart();loadQuotes();}
  },
  onSnapshot(data){backendConnected=true;receivedAt=performance.now();render(data);$("latency").textContent="WSS · canlı veri bağlantısı";},
  onQuotes(data){quoteTransport=data.transport||"UNAVAILABLE";const members=new Set(quoteMembers());for(const row of (data.items||[]).slice(0,128))if(members.has(row.symbol))remember(row);renderSymbols();},
  onUpdate({operation,params,data,error}){
    if(operation==="symbols"&&params.search===$("search").value.trim().toUpperCase()&&params.offset===offset){if(error){symbolStatus="error";renderSymbols();}else showSymbols(data);}
    if(operation==="chart"&&params.symbol===selected&&params.timeframe===timeframe){if(error)chartFailure();else showChart(data);}
  },
  onDisconnect(){canControl=false;quoteTransport="UNAVAILABLE";symbolStatus="error";for(const request of requests.values())request.abort();stale("Canlı bağlantı kesildi; yeniden senkronizasyon bekleniyor.");renderSymbols();}
});
transport.connect();
