"use strict";
/* Read-only UI projections: quant-owned execution is verified from server telemetry. */
(() => {
  let lastEnvironment = "";
  let pending = false;
  const $ = id => document.getElementById(id);

  function currentSnapshot() {
    // The existing classic app.js shares the global lexical binding `snapshot`.
    // Never fetch an extra full database snapshot merely to update these labels.
    try {return typeof snapshot === "object" ? snapshot : null;}
    catch {return null;}
  }

  function environmentName() {
    const fromFeed = currentSnapshot()?.market_environment;
    const inferred = typeof fromFeed === "string" ? fromFeed.toLowerCase() : "";
    return ["testnet", "live"].includes(lastEnvironment) ? lastEnvironment :
      ["testnet", "live"].includes(inferred) ? inferred : "";
  }

  function setText(element, value) {
    if (element && element.textContent !== value) element.textContent = value;
  }

  function removeLegacyLiveBadge() {
    const badge = document.querySelector(".rail-bottom .badge");
    if (badge?.textContent?.trim().toUpperCase() === "LIVE=FALSE") badge.remove();
  }

  function strategyState() {
    const data = currentSnapshot();
    const quant = (data?.sections?.workers?.data || []).find(
      row => row.service === "quant_core"
    );
    if (!quant || quant.freshness !== "OK") {
      return {note:"Quant Core'un güncel ölçümü bekleniyor; strateji sonucu henüz doğrulanmadı.", rows:[]};
    }
    const actual = quant.last_evaluation?.symbol_results || quant.symbol_results || [];
    if (Array.isArray(actual) && actual.length) return null; // app.js owns real results.
    const selected = Number(quant.selected_deep_count || 0);
    const ready = Number(quant.ready_deep_count || 0);
    let note;
    if (quant.analysis_ready !== true) {
      note = "Strateji analizi hazır değil: " + String(quant.reason || "Girdi bekleniyor") + ".";
    } else if (selected === 0) {
      note = "Henüz derin izleme için parite seçilmedi (0); gerçek strateji değerlendirmesi oluşturulmadı.";
    } else if (ready === 0) {
      note = `${selected} izleme paritesi seçilmiş; fakat emir defteri / işlem akışı kanıtı hazır değil.`;
    } else if (Array.isArray(quant.input_rejections) && quant.input_rejections.length) {
      note = `${ready} derin veri akışı hazır; ${quant.input_rejections.length} strateji girdisi reddedildi. Girdi reddi bölümüne bakın.`;
    } else {
      note = `${ready} derin izleme akışı hazır; henüz tamamlanmış strateji değerlendirmesi bulunmuyor.`;
    }
    const regime = quant.regime_label;
    if (regime === "DISORDER" || regime === "TRANSITION" || regime === "UNKNOWN") {
      note += " Piyasa rejimi: " + regime + ".";
    }
    const opportunities = (data?.sections?.opportunities?.data || []).filter(
      row => row.freshness === "OK" && row.score != null &&
        typeof row.symbol === "string" && Number.isFinite(Number(row.score))
    ).slice(0, 8);
    return {note, rows:opportunities};
  }

  function renderStrategyPreview() {
    const context = strategyState();
    if (!context) return;
    setText($("engine-pair-note"), context.note);
    const target = $("engine-preview");
    if (!target || target.querySelector(".tf-pretrade-preview")) return;
    const wrapper = document.createElement("div");
    wrapper.className = "tf-pretrade-preview";
    const heading = document.createElement("p");
    heading.className = "source-note";
    heading.textContent = context.rows.length ?
      "Strateji değerlendirmesi değil: izleme önceliğine göre güncel fırsat puanları." :
      "Doğrulanmış strateji sonucu yok; koşullar oluştuğunda bu alan güncellenecek.";
    wrapper.append(heading);
    if (context.rows.length) {
      const table = document.createElement("table");
      table.setAttribute("aria-label", "Strateji öncesi izleme adayları");
      const head = document.createElement("thead");
      const header = document.createElement("tr");
      for (const name of ["İZLEME ADAYI", "GÖZLEM SKORU", "STRATEJİ KARARI"]) {
        const cell = document.createElement("th");
        cell.textContent = name;
        header.append(cell);
      }
      head.append(header);
      table.append(head);
      const body = document.createElement("tbody");
      for (const row of context.rows) {
        const tr = document.createElement("tr");
        for (const value of [row.symbol, Number(row.score).toFixed(4), "Henüz yok"]) {
          const cell = document.createElement("td");
          cell.textContent = value;
          tr.append(cell);
        }
        body.append(tr);
      }
      table.append(body);
      wrapper.append(table);
    }
    target.replaceChildren(wrapper);
  }

  function sync() {
    removeLegacyLiveBadge();
    const environment = environmentName();
    const execution = currentSnapshot()?.execution_readiness;
    const worker = (currentSnapshot()?.sections?.workers?.data || []).find(
      row => row.service === "execution_core" && row.freshness === "OK"
    );
    const enabled = worker?.entries_enabled === true;
    const modeText = environment === "testnet" ?
      (execution?.ready === true && enabled ? "TESTNET · OTOMATİK EMİR AKTİF" :
        enabled ? "TESTNET · EMİR AKIŞI BEKLİYOR" : "TESTNET · EMİR PASİF") :
      environment === "live" ? "CANLI PİYASA VERİSİ" : "Ortam ölçümü bekleniyor";
    setText($("runtime-mode"), modeText);
    renderStrategyPreview();
  }

  function schedule() {
    if (pending) return;
    pending = true;
    queueMicrotask(() => {pending = false; sync();});
  }

  for (const id of ["runtime-mode",  "engine-pairs", "engine-pair-note", "engine-preview"]) {
    const element = $(id);
    if (element) new MutationObserver(schedule).observe(
      element, {subtree:true, childList:true, characterData:true}
    );
  }
  sync();
})();
