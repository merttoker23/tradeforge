"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const names = {testnet:"TESTNET",live:"LIVE"};
  let selected = "testnet", busy = false;
  const status = message => {$("binance-feedback").textContent = message;};
  async function call(path, method="GET", body=null) {
    const response = await fetch(path, {
      method,
      headers: method === "POST" ? {"Content-Type":"application/json"} : {},
      body: body === null ? undefined : JSON.stringify(body),
      credentials:"same-origin", cache:"no-store", redirect:"error", referrerPolicy:"no-referrer"
    });
    if (!response.ok) throw new Error(response.status === 403 ? "Yetki veya oturum doğrulanamadı" :
                                           response.status === 400 ? "İşlem reddedildi; açık pozisyon/emir veya risk kilidini kontrol edin" :
                                           "Bağlantı veya sunucu hatası");
    return await response.json();
  }
  async function refresh() {
    try {
      const state = await call("/api/binance/settings");
      selected = state.environment;
      $("binance-environment").value = selected;
      $("binance-current").textContent = names[state.environment] || "BİLİNMİYOR";
      $("binance-revision").textContent = String(state.revision);
      for (const env of ["testnet","live"]) {
        const value = state.credentials[env];
        $("binance-"+env+"-status").textContent = value.configured ?
          (value.last_test_at ? (value.connected ? "Son bağlantı testi başarılı" : "Son bağlantı testi başarısız") : "Anahtar kayıtlı; test edilmedi") :
          "API anahtarı girilmedi";
      }
      status("Canlı emir izni: KAPALI. Ortam değişimi gerçek emir yürütmeyi açmaz.");
    } catch (error) { status("Ayarlar alınamadı: " + error.message); }
  }
  async function action(callback) {
    if (busy) return;
    busy = true;
    document.querySelectorAll("#binance-screen button").forEach(button => button.disabled = true);
    try { await callback(); await refresh(); }
    catch (error) { status("İşlem tamamlanamadı: " + error.message); }
    finally {busy = false;document.querySelectorAll("#binance-screen button").forEach(button => button.disabled = false);}
  }
  for (const env of ["testnet","live"]) {
    $("binance-"+env+"-save").addEventListener("click", () => action(async () => {
      const key = $("binance-"+env+"-key"), secret = $("binance-"+env+"-secret");
      await call("/api/binance/settings","POST",{environment:env,api_key:key.value,api_secret:secret.value});
      key.value = ""; secret.value = "";
      status(names[env] + " kimlik bilgileri güvenli kaydedildi.");
    }));
    $("binance-"+env+"-test").addEventListener("click", () => action(async () => {
      const result = await call("/api/binance/test","POST",{environment:env});
      if (!result.connected) throw new Error(names[env] + " REST hesap testi başarısız");
    }));
  }
  $("binance-activate").addEventListener("click", () => action(async () => {
    const target = $("binance-environment").value;
    if (!confirm("Ortam " + names[target] + " olarak değiştirilsin mi? İşlem kilidi açık olmalı; servisler yeniden bağlanır.")) return;
    await call("/api/binance/settings","POST",{environment:target});
    location.reload();
  }));
  document.querySelector('[data-screen="binance"]').addEventListener("click", refresh);
})();
