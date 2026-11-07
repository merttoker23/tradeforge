"use strict";

(() => {
  const form = document.getElementById("login-form");
  const key = document.getElementById("session-key");
  const toggle = document.getElementById("toggle-key");
  const submit = document.getElementById("login-submit");
  const label = document.getElementById("submit-label");
  const error = document.getElementById("login-error");
  const status = document.getElementById("login-status");
  const spinner = form.querySelector(".spinner");
  let pending = false;
  let generation = 0;
  let activeRequest = null;

  function hideKey() {
    key.type = "password";
    toggle.setAttribute("aria-pressed", "false");
    toggle.setAttribute("aria-label", "Oturum anahtarını göster");
  }

  function setPending(value) {
    pending = value;
    form.setAttribute("aria-busy", String(value));
    submit.disabled = key.disabled = toggle.disabled = value;
    spinner.hidden = !value;
    label.textContent = value ? "Oturum doğrulanıyor…" : "Güvenli giriş yap";
    status.textContent = value ? "Oturum anahtarınız doğrulanıyor. Lütfen bekleyin." : "";
  }

  function clearError() {
    error.textContent = "";
    key.removeAttribute("aria-invalid");
  }

  toggle.hidden = false;
  setPending(false);
  // Keep the native POST contract; JavaScript supplies origin-safe submission.
  form.noValidate = true;
  toggle.addEventListener("click", () => {
    if (key.type === "text") {
      hideKey();
    } else {
      key.type = "text";
      toggle.setAttribute("aria-pressed", "true");
      toggle.setAttribute("aria-label", "Oturum anahtarını gizle");
    }
  });
  key.addEventListener("input", clearError);
  window.addEventListener("blur", hideKey);
  window.addEventListener("pageshow", event => {
    if (event.persisted) {
      generation += 1;
      activeRequest?.abort();
      activeRequest = null;
      key.value = "";
      hideKey();
      clearError();
      setPending(false);
    }
  });

  form.addEventListener("submit", async event => {
    event.preventDefault();
    if (pending) return;
    clearError();
    if (!key.validity.valid) {
      key.setAttribute("aria-invalid", "true");
      key.focus();
      error.textContent = key.value
        ? "Anahtar 43–128 karakter olmalı; yalnızca harf, rakam, alt çizgi ve kısa çizgi içerebilir."
        : "Devam etmek için oturum anahtarınızı girin.";
      return;
    }

    const body = new FormData(form);
    hideKey();
    setPending(true);
    const controller = new AbortController();
    activeRequest = controller;
    const attempt = ++generation;
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch("/login", {
        method: "POST",
        body,
        credentials: "same-origin",
        signal: controller.signal,
      });
      if (attempt !== generation) return;
      // The existing server issues the HttpOnly cookie and redirects to the terminal.
      if (response.ok && response.redirected) {
        const target = new URL(response.url);
        if (target.origin === window.location.origin && target.pathname === "/") {
          key.value = "";
          window.location.replace("/");
          return;
        }
      }
      const messages = {
        401: "Oturum anahtarı doğrulanamadı. Anahtarınızı kontrol edip tekrar deneyin.",
        403: "Giriş isteği doğrulanamadı. Sayfayı yenileyip tekrar deneyin.",
        429: "Çok fazla giriş denemesi yapıldı. Bir dakika bekleyip tekrar deneyin.",
      };
      setPending(false);
      if (response.status === 401) {
        key.setAttribute("aria-invalid", "true");
        key.focus();
      }
      error.textContent = messages[response.status]
        || "Giriş hizmeti geçici olarak kullanılamıyor. Lütfen tekrar deneyin.";
    } catch {
      if (attempt !== generation) return;
      setPending(false);
      error.textContent = "Bağlantı kurulamadı. Bağlantınızı kontrol edip tekrar deneyin.";
    } finally {
      clearTimeout(timeout);
      if (activeRequest === controller) activeRequest = null;
    }
  });
})();
