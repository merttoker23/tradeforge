"use strict";
/* Testnet-only manual operator controls. No credentials, leverage guesses or autosubmit. */
(() => {
  const terminal = document.getElementById("terminal-screen");
  if (!terminal || document.getElementById("demo-manual-order")) return;
  const panel = document.createElement("article");
  panel.id = "demo-manual-order";
  panel.className = "panel";
  panel.innerHTML = `
    <div class="panel-head"><h2>Testnet · Manuel emir</h2><span class="muted">Yalnızca demo borsası · otomatik strateji emri değil</span></div>
    <p id="demo-manual-status" class="source-note" role="status">Demo hesabı kontrol ediliyor…</p>
    <form id="demo-manual-form">
      <label>Sembol<input name="symbol" maxlength="40" required placeholder="BTCUSDT" autocomplete="off"></label>
      <label>Yön<select name="side"><option>BUY</option><option>SELL</option></select></label>
      <label>Emir türü<select name="type"><option>MARKET</option><option>LIMIT</option></select></label>
      <label>Miktar (coin)<input name="quantity" required placeholder="0.001" inputmode="decimal" autocomplete="off"></label>
      <label id="demo-manual-price-label" hidden>Limit fiyatı (USDT)<input name="price" placeholder="50000" inputmode="decimal" autocomplete="off"></label>
      <label>Pozisyon modu<select name="positionSide"><option>BOTH</option><option>LONG</option><option>SHORT</option></select></label>
      <label><input name="reduceOnly" type="checkbox"> Reduce only (BOTH)</label>
      <button type="submit">Demo emir gönder</button>
    </form>
    <p class="source-note">Borsa miktar, adım, fiyat ve marjin kurallarını uygular. Başarısız ya da belirsiz bir yanıttan sonra aynı emri otomatik tekrar gönderme.</p>
    <pre id="demo-manual-result"></pre>
    <form id="demo-manual-check">
      <label>Emir kimliği<input name="client_order_id" maxlength="36" placeholder="tfmanual_…" autocomplete="off" required></label>
      <button type="submit">Emir durumunu sorgula</button>
      <button type="button" id="demo-manual-cancel">Demo emrini iptal et</button>
    </form>`;
  terminal.append(panel);
  const form = panel.querySelector("#demo-manual-form");
  const status = panel.querySelector("#demo-manual-status");
  const output = panel.querySelector("#demo-manual-result");
  const button = form.querySelector('button[type="submit"]');
  button.disabled = true; // Wait for verified Testnet environment/capability.
  const priceLabel = panel.querySelector("#demo-manual-price-label");
  const kind = form.elements.namedItem("type");
  kind.addEventListener("change", () => {priceLabel.hidden = kind.value !== "LIMIT";});
  const symbolInput = form.elements.namedItem("symbol");
  let active = false;
  async function state() {
    try {
      const res = await fetch("/api/demo/account", {cache:"no-store", credentials:"same-origin"});
      if (!res.ok) throw new Error("HTTP " + res.status);
      const data = await res.json();
      const isDemo = data.environment === "testnet";
      active = data.manual_orders_available === true && isDemo;
      window.dispatchEvent(new CustomEvent("tradeforge:demo-account", {
        detail: {environment:data.environment, status:data.status,
                 manual_orders_available:active}
      }));
      status.textContent = isDemo ?
        data.status === "OK" ? `Demo hesabı güncel · Bakiye ${data.equity} USDT · Pozisyon ${data.positions} · ${active ? "Manuel emir formu açık" : "Emir yetkisi yok"}` :
        data.status === "BINANCE_MASTER_REQUIRED" ? "Hesap anahtarını çözmek için mevcut binance-master gerekli." :
        "Testnet seçili · Hesap doğrulanamadı (" + (data.status || "UNKNOWN") + ")" :
        "Demo emirleri için Testnet seçilmelidir. Canlı borsaya emir iletilmez.";
      button.disabled = !active;
      const selected = document.getElementById("selected-symbol")?.textContent?.trim();
      if (selected && /^[A-Z0-9_]{2,40}$/.test(selected) && document.activeElement !== symbolInput && !symbolInput.dataset.changed)
        symbolInput.value = selected;
    } catch {
      active = false;
      button.disabled = true;
      window.dispatchEvent(new CustomEvent("tradeforge:demo-unavailable"));
      status.textContent = "Demo hesap durumuna ulaşılamıyor; emir gönderimi doğrulanamıyor.";
    }
  }
  symbolInput.addEventListener("input", () => {symbolInput.dataset.changed = "true";});
  form.addEventListener("submit", async event => {
    event.preventDefault();
    if (!active) {output.textContent = "Testnet ortamı doğrulanmadı.";return;}
    const data = Object.fromEntries(new FormData(form).entries());
    data.symbol = String(data.symbol || "").trim().toUpperCase();
    data.quantity = String(data.quantity || "").trim();
    data.reduceOnly = form.elements.namedItem("reduceOnly").checked;
    if (data.type !== "LIMIT") delete data.price;
    else {data.price = String(data.price || "").trim();data.timeInForce = "GTC";}
    if (!window.confirm(`${data.symbol} ${data.side} ${data.type}, miktar ${data.quantity}${data.price ? `, fiyat ${data.price}` : ""}: DEMO emrini göndermek istiyor musun?`)) return;
    button.disabled = true;
    output.textContent = "Emir bir kez gönderiliyor…";
    try {
      const res = await fetch("/api/demo/order", {method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data),credentials:"same-origin",cache:"no-store",redirect:"error"});
      const reply = await res.json();
      output.textContent = JSON.stringify(reply, null, 2);
      if (reply.client_order_id) checkId.value = reply.client_order_id;
      if (res.status === 202) output.textContent += "\nACK_UNKNOWN: Emir borsada gerçekleşmiş olabilir. Tekrar gönderme; client_order_id ile durumunu kontrol et.";
    } catch {output.textContent = "Bağlantı belirsiz: Emri otomatik tekrar gönderme; borsada emirlerini kontrol et.";}
    finally {button.disabled = false;}
  });
  const checker = panel.querySelector("#demo-manual-check");
  const checkId = checker.elements.namedItem("client_order_id");
  checker.addEventListener("submit", async event => {
    event.preventDefault();
    const symbol = symbolInput.value.trim().toUpperCase(), id = checkId.value.trim();
    try {
      const res = await fetch("/api/demo/order?" + new URLSearchParams({symbol,client_order_id:id}), {credentials:"same-origin",cache:"no-store"});
      output.textContent = JSON.stringify(await res.json(), null, 2);
    } catch {output.textContent = "Emir sorgulaması başarısız.";}
  });
  panel.querySelector("#demo-manual-cancel").addEventListener("click", async () => {
    const symbol = symbolInput.value.trim().toUpperCase(), id = checkId.value.trim();
    if (!id || !window.confirm(symbol + " / " + id + " Demo emri iptal edilsin mi?")) return;
    try {
      const res = await fetch("/api/demo/cancel",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({symbol,client_order_id:id}),credentials:"same-origin",cache:"no-store"});
      output.textContent = JSON.stringify(await res.json(), null, 2);
    } catch {output.textContent = "İptal yanıtı belirsiz. Borsadan emir durumunu kontrol et.";}
  });
  state();
  setInterval(state, 5000);
})();
