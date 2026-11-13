<div align="center">

# Tradeforge 2.0

**Algoritmik Kripto Piyasası Analiz ve İşlem Platformu**

[![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Pydantic](https://img.shields.io/badge/Pydantic-2-E92063?logo=pydantic&logoColor=white)](https://docs.pydantic.dev/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![uv](https://img.shields.io/badge/uv-Paket%20Y%C3%B6netimi-DE5FE9?logo=astral&logoColor=white)](https://docs.astral.sh/uv/)

</div>

## Proje Hakkında

**Tradeforge 2.0**, Binance USDⓈ-M USDT sürekli vadeli işlem piyasalarını izlemek, piyasa koşullarını değerlendirmek ve risk odaklı algoritmik işlem kararları üretmek amacıyla geliştirilen modüler bir platformdur. Tek VPS üzerinde çalışacak şekilde tasarlanan sistem; veri bütünlüğünü, açıklanabilir kararları ve güvenli işlem yürütmeyi esas alır.

## Öne Çıkan Özellikler

- **Dinamik piyasa keşfi** — Uygun vadeli işlem sözleşmelerinin borsa verilerinden belirlenmesi ve doğrulanması.
- **Piyasa veri altyapısı** — REST ve WebSocket üzerinden kademeli veri toplama, bağlantı yenileme ve veri tazeliği kontrolleri.
- **Kanonik piyasa durumu** — Kapanmış mumlar, emir defteri ve işlem akışından zaman tutarlı piyasa gözlemleri.
- **Piyasa zekâsı** — Rejim, volatilite, göreceli güç ve piyasa genişliği üzerinden bütüncül analiz.
- **Adaptif fırsat taraması** — Piyasa genelinden seçilen adayların kademeli izlenmesi ve momentum temelli fırsat değerlendirmesi.
- **Kalıcı durum ve denetim** — PostgreSQL tabanlı kayıtlar, işlem bütünlüğü ve izlenebilir olay altyapısı.
- **Canlı kontrol merkezi** — Piyasa, grafik, hesap/PnL, emir ve sistem verileri ile denetimli operatör komutları tek oturumlu WebSocket bağlantısından geçer; önyüz AJAX yoklaması veya HTTP fallback kullanmaz.
- **Güvenli varsayılanlar** — Gerçek emir yetkisi kapalı, veri veya kanıt eksikliğinde işlem üretmeyen yaklaşım.

Nginx arkasında çalışan mevcut VPS kurulumlarında HTTPS `server` bloğuna
`deploy/nginx/dashboard-websocket.conf` içeriği eklenmelidir. WebSocket yolu,
`Upgrade` ve `Connection: upgrade` başlıklarıyla `127.0.0.1:18104` adresine
iletilir. Yapılandırma `sudo nginx -t` ile doğrulandıktan sonra
`sudo systemctl reload nginx` ile kesintisiz yüklenir. Otomatik imaj dağıtımı
VPS üzerindeki Nginx yapılandırmasını değiştirmez.

## Kullanılan Teknolojiler

| Katman | Teknolojiler |
| --- | --- |
| Uygulama | Python 3.12+ · asyncio · Pydantic 2 |
| Piyasa bağlantısı | aiohttp · Binance USDⓈ-M API |
| Veritabanı | PostgreSQL 17 · SQLAlchemy 2 · Alembic |
| Altyapı | Docker Compose · uv |
| Test ve kalite | pytest · Ruff · mypy · GitHub Actions |

## Hızlı Kurulum

**Gereksinimler:** Git, Python 3.12+ ve uv. Veritabanı entegrasyon testleri ve piyasa akışı için ayrıca PostgreSQL gereklidir.

```bash
git clone https://github.com/merttoker23/tradeforge.git
cd tradeforge
uv sync --locked
```

Kurulum sonrasında servislerin güvenli, çevrimdışı tanılama kontrollerini çalıştırabilirsiniz:

```bash
uv run --frozen --offline python -m apps.market_gateway --check
uv run --frozen --offline python -m apps.quant_core --check
uv run --frozen --offline python -m apps.execution_guardian --check
uv run --frozen --offline python -m apps.dashboard --check
```

Bu komutlar gerçek emir göndermez ve işlem yapmaya hazır olunduğunu göstermez. API anahtarı gerektirmez.

## Localhost üzerinde Binance Futures Testnet

Şeması güncel PostgreSQL çalışıyor olmalı. Panelden Binance ortamını `testnet` seçin ve
Demo hesabı anahtarını kaydedin. Üç süreç aynı yerel PostgreSQL URL dosyasını kullanır.
Gateway ve quant için token dosyası, panelde kayıtlı Testnet anahtarını şifreleyen
master dosyasıyla aynı olmalıdır. Dosyaları çalışma ağacının dışında veya Git tarafından
yok sayılan bir dizinde, yalnızca operatörün erişebileceği şekilde saklayın.

Aşağıdaki PowerShell ayarlarını her servis terminalinde uygulayın. Dizinler mevcut
Windows kurulumuna örnektir. `TF_POSTGRES_VOLUME` gerçek PostgreSQL veri diskini ölçer.

```powershell
$env:TRADEFORGE_MODE = 'testnet'
$env:TRADEFORGE_LIVE = 'false'
$env:TF_POSTGRES_VOLUME = (Resolve-Path .\var\postgres\data).Path
$dbFile = (Resolve-Path .\var\runtime\database.url).Path
$masterFile = (Resolve-Path .\var\runtime\binance_master).Path
```

Üç ayrı terminalde sırasıyla şu servisleri çalıştırın.

```powershell
uv run --frozen --offline python -m apps.market_gateway --stream --database-url-file $dbFile --gateway-token-file $masterFile
```

```powershell
$env:TF_AUTO_ORDERS = 'true'
$env:TF_TESTNET_TRIAL = 'false'
uv run --frozen --offline python -m apps.quant_core --runtime --database-file $dbFile --gateway-token-file $masterFile --gateway-base http://127.0.0.1:8101
```

```powershell
uv run --frozen --offline python -m apps.dashboard --database-file $dbFile --binance-master-file $masterFile
```

Panel `http://127.0.0.1:8104` adresinde, gateway ve quant durum uçları sırasıyla
`8101` ve `8102` portlarında `/health` ve `/readiness` altında çalışır. `TF_AUTO_ORDERS=true`
geçerli sinyal, tetik, hesap mutabakatı, risk onayı ve istatistiksel kanıt olduğunda
Testnet girişine açık izin verir. Sınırlı deneme için ayrıca `TF_TESTNET_TRIAL=true`
ayarlanır. Deneme en çok 20 girişle sınırlıdır. Varsayılan istatistiksel değerlendirme
100 etkin eğitim ve 100 etkin OOS örneği ister.

Anahtar veya Binance ortamı değiştiğinde servisleri yeniden başlatın. Paneldeki Testnet
durdurma komutu yeni girişleri durdurur ve açık pozisyonların koruma döngüsü çalışmaya
devam eder. Kalıcı risk kilidi açıksa, paneldeki sunucu kontrollü devam komutu ve hazır
hesap mutabakatı gerekir.

---

<div align="center">
  <sub><a href="https://github.com/merttoker23">Mert Toker</a> tarafından geliştirilmektedir.</sub>
</div>
