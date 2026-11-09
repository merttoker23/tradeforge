"use strict";
/* Read-only UI projections. This script never submits, authorizes, or replays orders.
   The server's fixed-Testnet order endpoint remains the only order authority. */
(() => {
  let lastEnvironment = "";
  let manualOrdersAvailable = false;
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

  function removeLegacyGuardianRows() {
    for (const id of ["services", "data-health"]) {
      const area = $(id);
      if (!area) continue;
      for (const row of area.querySelectorAll(".health-row")) {
        const label = row.querySelector("span")?.textContent?.trim()?.toLowerCase();
        if (label === "guardian" || label === "execution_guardian") row.remove();
      }
    }
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
    setText($("evaluation-note"), context.note);
    const target = $("analysis-results");
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
    removeLegacyGuardianRows();
    const environment = environmentName();
    const modeText = environment === "testnet" ? "TESTNET · DEMO" :
      environment === "live" ? "CANLI PİYASA VERİSİ" : "Ortam ölçümü bekleniyor";
    setText($("runtime-mode"), modeText);
    const manual = environment === "testnet" && manualOrdersAvailable;
    if (environment === "testnet") {
      setText($("execution-status"), manual ?
        "Manuel Testnet emir formu açık" : "Testnet emir bağlantısı doğrulanıyor");
      const execution = $("execution-status");
      if (execution) execution.classList.toggle("ready", manual);
      setText($("execution-note"), manual ?
        "Yalnızca kullanıcı onaylı Testnet manuel emirleri gönderilir; otomatik strateji emirleri ayrı durumdur." :
        "Testnet manuel emir uç noktası doğrulanıyor; otomatik emir yetkisi kapalıdır.");
    }
    const summary = $("guardian-summary");
    if (summary) {
      const label = summary.parentElement?.querySelector("span");
      setText(label, "Testnet manuel emir");
      setText(summary, manual ? "Form açık" : "Henüz doğrulanmadı");
    }
    renderStrategyPreview();
  }

  function schedule() {
    if (pending) return;
    pending = true;
    queueMicrotask(() => {pending = false; sync();});
  }

  window.addEventListener("tradeforge:demo-account", event => {
    const detail = event.detail || {};
    if (["testnet", "live"].includes(detail.environment)) lastEnvironment = detail.environment;
    manualOrdersAvailable = detail.manual_orders_available === true;
    schedule();
  });
  window.addEventListener("tradeforge:demo-unavailable", () => {
    manualOrdersAvailable = false;
    // Keep the last independently established environment rather than
    // incorrectly displaying "Doğrulanamadı" when one GET times out.
    schedule();
  });

  for (const id of ["runtime-mode", "services", "data-health", "guardian-summary",
    "execution-status", "execution-note", "analysis-results", "evaluation-note"]) {
    const element = $(id);
    if (element) new MutationObserver(schedule).observe(
      element, {subtree:true, childList:true, characterData:true}
    );
  }
  sync();
})();
