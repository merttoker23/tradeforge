"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const names = {testnet:"TESTNET",live:"LIVE"};
  const messages = {
    BINANCE_SETTINGS_INVALID:"Ayarlar geçersiz; ortam ve kimlik alanlarını denetleyin.",
    BINANCE_TEST_INVALID:"Test için ortam seçin.",
    BINANCE_CREDENTIALS_INVALID:"API anahtarı/secret geçersiz. Başta/sonda boşluk bırakmadan girin.",
    BINANCE_ENCRYPTION_SECRET_INVALID:"Sunucuda --binance-master en az 32 karakter olmalı; servis yeniden başlatılmalı.",
    BINANCE_ENVIRONMENT_INVALID:"Ortam geçersiz.",
    BINANCE_OPEN_EXPOSURE_BLOCKS_SWITCH:"Açık pozisyon veya bekleyen emir var; önce kapatılmalı.",
    BINANCE_UNPARTITIONED_HISTORY_BLOCKS_SWITCH:"Ortam değişimi bölünmemiş geçmiş nedeniyle engelli; mevcut veritabanında etkinleştirme yapılamaz.",
    BINANCE_RISK_KILL_SWITCH_REQUIRED:"Önce risk kilidini (kill switch) etkinleştirin."
  };
  let selected = "testnet", busy = false;
  const status = message => {$("binance-feedback").textContent = message;};
  async function call(path, method="GET", body=null) {
    const response = await fetch(path, {
      method,
      headers: method === "POST" ? {"Content-Type":"application/json"} : {},
      body: body === null ? undefined : JSON.stringify(body),
      credentials:"same-origin", cache:"no-store", redirect:"error", referrerPolicy:"no-referrer"
    });
    let payload = null;
    try {payload = await response.json();} catch {}
    if (!response.ok) {
      const code = typeof payload?.error === "string" ? payload.error : null;
      if (response.status === 403) throw new Error("Yetki veya oturum doğrulanamadı");
      if (code && messages[code]) throw new Error(messages[code]);
      if (response.status === 400) throw new Error("İşlem reddedildi; girdi ve kilit durumunu denetleyin");
      if (response.status === 503) throw new Error("Sunucu ayarları (DB / binance-master) hazır değil");
      throw new Error("Bağlantı veya sunucu hatası");
    }
    if (payload === null || typeof payload !== "object") throw new Error("Sunucu yanıtı geçersiz");
    return payload;
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
      const apiKey = key.value.trim(), apiSecret = secret.value.trim();
      if (!apiKey || !apiSecret) throw new Error("API anahtarı ve secret girin.");
      await call("/api/binance/settings","POST",{environment:env,api_key:apiKey,api_secret:apiSecret});
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
    if (!confirm("Ortam " + names[target] + " olarak değiştirilsin mi? Risk kilidi etkin olmalı; servisler yeniden bağlanır.")) return;
    await call("/api/binance/settings","POST",{environment:target});
    location.reload();
  }));
  document.querySelector('[data-screen="binance"]').addEventListener("click", refresh);
})();
