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
    BINANCE_UNPARTITIONED_HISTORY_BLOCKS_SWITCH:"LIVE ortamı bölünmemiş geçmiş nedeniyle etkinleştirilemez; Testnet seçilebilir.",
    BINANCE_RISK_KILL_SWITCH_REQUIRED:"Önce risk kilidini (kill switch) etkinleştirin.",
    BINANCE_TEST_CREDENTIALS_MISSING:"Bu ortam için kayıtlı API anahtarı yok; önce API Key ve API Secret girip kaydedin.",
    BINANCE_TEST_CREDENTIALS_INVALID:"Kayıtlı API anahtarı/secret okunamadı; bilgileri yeniden girin.",
    BINANCE_TEST_API_KEY_INVALID:"API anahtarı Binance tarafından geçersiz sayıldı. Anahtarın doğru ortama (Testnet/Live) ait ve silinmemiş olduğunu denetleyin.",
    BINANCE_TEST_PERMISSION_DENIED:"API anahtarının izinleri veya IP kısıtı bu bağlantıya kapalı.",
    BINANCE_TEST_SIGNATURE_INVALID:"API Secret eşleşmiyor; API Key ve Secret birlikte yeniden kaydedin.",
    BINANCE_TEST_CLOCK_UNSAFE:"Sunucu saati Binance ile uyumlu değil; saat eşitlemesini düzeltin.",
    BINANCE_TEST_RATE_LIMITED:"Binance istek sınırına takıldı; kısa süre sonra tekrar deneyin.",
    BINANCE_TEST_REJECTED:"Binance REST isteğini reddetti.",
    BINANCE_TEST_NETWORK_UNAVAILABLE:"Binance REST sunucusuna ağ bağlantısı kurulamadı.",
    BINANCE_TEST_RESPONSE_INVALID:"Binance yanıtı beklenen biçimde değil.",
    BINANCE_TEST_FAILED:"REST bağlantı testi tamamlanamadı."
  };
  const MASK = "••••••••••••••••";
  const active = {environment:"", credentials:{}};
  let environmentDirty = false, busy = false;
  const masked = element => element.dataset.masked === "true";
  const status = message => {$('binance-feedback').textContent = message;};
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
      // A 401/403 originates from the dashboard permission boundary, not Binance.
      // Never retry a credential write without verified authorization or clear inputs.
      if (response.status === 401 || response.status === 403) {
        const detail = code === "SCOPE_REQUIRED" ?
          "Bu oturumda API ayarlarını değiştirme yetkisi yok." :
          response.status === 401 ? "Oturum süresi dolmuş veya geçersiz." :
          "İstek sunucunun güvenlik denetimi tarafından reddedildi (403).";
        throw new Error(detail + " İşlem sunucuya kaydedilmedi. Yetkili panel bağlantısını doğrulayın.");
      }
      if (code && messages[code]) throw new Error(messages[code]);
      if (response.status === 400) throw new Error("İşlem reddedildi; girdi ve kilit durumunu denetleyin");
      if (response.status === 503) throw new Error("Sunucu ayarları (DB / binance-master) hazır değil");
      throw new Error("Bağlantı veya sunucu hatası");
    }
    if (payload === null || typeof payload !== "object") throw new Error("Sunucu yanıtı geçersiz");
    return payload;
  }
  function setMask(element, on) {
    if (on && !masked(element) && element.value.trim() === "") {
      element.value = MASK;
      element.dataset.masked = "true";
      element.classList.add("filled");
    } else if (!on && masked(element)) {
      element.value = "";
      delete element.dataset.masked;
      element.classList.remove("filled");
    }
  }
  function entryState(env) {
    const key = $("binance-"+env+"-key"), secret = $("binance-"+env+"-secret");
    const apiKey = masked(key) ? "" : key.value.trim();
    const apiSecret = masked(secret) ? "" : secret.value.trim();
    if (!masked(key)) key.classList.toggle("filled", apiKey !== "");
    if (!masked(secret)) secret.classList.toggle("filled", apiSecret !== "");
    $("binance-"+env+"-entry").textContent = masked(key) && masked(secret) ? "" :
      apiKey && apiSecret ? "Yeni API Key ve Secret girildi; kaydetmek için Kaydet'e basın." :
      apiKey ? "API Secret bekleniyor." : apiSecret ? "API Key bekleniyor." : "";
    return {apiKey, apiSecret};
  }
  function clearEntry(env) {
    for (const field of ["key","secret"]) {
      const element = $("binance-"+env+"-"+field);
      element.value = "";
      delete element.dataset.masked;
      delete element.dataset.typed;
      element.classList.remove("filled");
    }
    entryState(env);
  }
  function prefill(env) {
    const configured = (active.credentials[env] || {}).configured === true;
    for (const field of ["key","secret"]) {
      const element = $("binance-"+env+"-"+field);
      if (!configured) {
        setMask(element, false);
      } else if (!masked(element) && element.value.trim() === "" &&
                 element.dataset.typed !== "true" && document.activeElement !== element) {
        setMask(element, true);
      }
    }
    entryState(env);
  }
  function environmentNote() {
    const target = $("binance-environment").value;
    $("binance-environment-note").textContent =
      target === active.environment ? "" :
      names[target] + " seçildi; kaydetmek için \"Ortamı etkinleştir\"e basın.";
  }
  async function refresh() {
    const state = await call("/api/binance/settings");
    active.environment = state.environment;
    active.credentials = state.credentials || {};
    if (!environmentDirty) $("binance-environment").value = state.environment;
    environmentNote();
    $("binance-current").textContent = names[state.environment] || "BİLİNMİYOR";
    for (const env of ["testnet","live"]) {
      const value = active.credentials[env] || {};
      $("binance-"+env+"-status").textContent = value.configured ?
        (value.last_test_at ? (value.connected ? "Son bağlantı testi başarılı" : "Son bağlantı testi başarısız") : "Anahtar kayıtlı; test edilmedi") :
        "API anahtarı girilmedi";
      prefill(env);
    }
  }
  async function load() {
    try {await refresh();}
    catch (error) {status("Ayarlar alınamadı: " + error.message);}
  }
  async function action(callback) {
    if (busy) return;
    busy = true;
    document.querySelectorAll("#binance-screen button").forEach(button => button.disabled = true);
    try {
      let message = null, failure = null;
      try {message = await callback();}
      catch (error) {failure = error;}
      try {await refresh();} catch {}
      if (failure) throw failure;
      if (message) status(message);
    }
    catch (error) { status("İşlem tamamlanamadı: " + error.message); }
    finally {busy = false;document.querySelectorAll("#binance-screen button").forEach(button => button.disabled = false);}
  }
  for (const env of ["testnet","live"]) {
    for (const field of ["key","secret"]) {
      const element = $("binance-"+env+"-"+field);
      element.addEventListener("focus", () => {if (masked(element)) setMask(element, false);});
      element.addEventListener("input", () => {
        element.dataset.typed = element.value.trim() ? "true" : "false";
        entryState(env);
      });
      element.addEventListener("blur", () => {
        if (element.value.trim() === "" && (active.credentials[env] || {}).configured === true) {
          delete element.dataset.typed;
          setMask(element, true);
        }
        entryState(env);
      });
    }
    $("binance-"+env+"-save").addEventListener("click", () => action(async () => {
      const {apiKey, apiSecret} = entryState(env);
      if (!apiKey || !apiSecret) throw new Error("Kaydetmek için API Key ve API Secret birlikte girin.");
      await call("/api/binance/settings","POST",{environment:env,api_key:apiKey,api_secret:apiSecret});
      clearEntry(env);
      return names[env] + " kimlik bilgileri güvenli kaydedildi.";
    }));
    $("binance-"+env+"-test").addEventListener("click", () => action(async () => {
      const {apiKey, apiSecret} = entryState(env);
      if (apiKey || apiSecret) {
        if (!apiKey || !apiSecret) throw new Error("REST testi için API Key ve API Secret birlikte girilmeli.");
        await call("/api/binance/settings","POST",{environment:env,api_key:apiKey,api_secret:apiSecret});
        clearEntry(env);
      } else if (!(active.credentials[env] || {}).configured) {
        throw new Error(names[env] + " için API anahtarı girilmedi; API Key ve Secret girip kaydedin.");
      }
      const result = await call("/api/binance/test","POST",{environment:env});
      if (!result.connected) throw new Error(messages[result.reason] || (names[env] + " REST hesap testi başarısız"));
      return names[env] + " REST bağlantı testi başarılı.";
    }));
  }
  $("binance-environment").addEventListener("change", () => {
    environmentDirty = true;
    environmentNote();
  });
  $("binance-activate").addEventListener("click", () => action(async () => {
    const target = $("binance-environment").value;
    if (target === active.environment) {
      environmentDirty = false;
      environmentNote();
      return names[target] + " zaten etkin çalışma ortamı.";
    }
    if (!confirm("Çalışma ortamı " + names[target] + " olarak değiştirilsin mi? Risk kilidi etkin olmalı; servisler yeniden bağlanır.")) return null;
    await call("/api/binance/settings","POST",{environment:target});
    environmentDirty = false;
    return "Çalışma ortamı " + names[target] + " olarak kaydedildi.";
  }));
  document.querySelector('[data-screen="binance"]').addEventListener("click", load);
  load();
})();
