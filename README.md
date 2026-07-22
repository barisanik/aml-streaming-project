# Gerçek Zamanlı Kara Para Tespit Platformu
(TR 🇹🇷 | [English below](#real-time-money-laundering-detection-platform))

> Proje Durumu: Devam ediyor.

Bu proje sentetik banka işlem akışı üreten, bu akışı Kafka uyumlu bir event log (Redpanda) üzerinden gerçek zamanlı tüketen, kural tabanlı AML ve fraud tespiti yapan, sonuçları dbt incremental modelleriyle analitik katmana işleyen ve Grafana ile canlı izlenen uçtan uca bir streaming veri platformunu içermektedir.

Sistem işlemleri saniyeler içinde değerlendirir, şüpheli davranış kalıplarını yakalar ve her alert'e hangi kuralın neden tetiklendiğini açıklayan bir gerekçe ekler.

## Kullanılan Teknolojiler

| Katman | Teknoloji |
|---|---|
| Mesaj sistemi | Redpanda |
| Streaming client | Python `confluent-kafka` |
| Şema ve validasyon | `pydantic` |
| Simülasyon | Python (`faker` ve `numpy`) |
| Sıcak veri | PostgreSQL |
| Dönüşüm | dbt-postgres, incremental modeller |
| Durum yönetimi | Redis (Docker) |
| İzleme | Grafana (Postgres'i veri kaynağı olarak kulanır) |
| Bildirim | Slack ve/veya SMTP |
| Test | pytest |
| CI/CD | GitHub Actions |
| Container | Docker Compose |

## Mimari

Platform altı ana bileşenden oluşur:

1. **Profil üretici**, binlerce müşteri profili oluşturur. Her profil lokasyon, gelir segmenti, ortalama işlem tutarı ve aktif saat aralığı gibi bilgiler taşır.
2. **İşlem üretici (producer)**, bu profilleri kullanarak Poisson süreciyle işlem akışı üretir ve Redpanda'daki `transactions` topic'ine yazar. Belirli bir olasılıkla gerçekçi fraud ve AML senaryoları da bu akışa karıştırılır.
3. **Kural motoru (consumer)**, işlemleri tüketir, pydantic ile doğrular ve hesap bazlı zaman pencereleri üzerinde kuralları değerlendirir. Doğrulanan işlemler Postgres'e yazılır; bir kural tetiklenirse alert önce Postgres'e, sonra Redpanda'daki `alerts` topic'ine yazılır.
4. **Bildirim servisi (notifier)**, üretilen alert'leri dinler ve Slack ve/veya e-posta üzerinden bildirim gönderir.
5. **dbt**, ham veriyi temizlenmiş ve özetlenmiş analitik tablolara dönüştürür.
6. **Grafana**, Postgres'ten okuduğu verilerle işlem hacmi, alert oranı, tespit gecikmesi gibi metrikleri canlı olarak görselleştirir ve sistemin sağlığını takip edilebilir kılar.

Tespit mantığı bellek içinde, analitik hesaplamalar ise SQL katmanında çalışır. Bu ayrım, hızlı tespit ile derinlemesine analizi birbirinden bağımsız tutar.

## Veri Kaynağı

Platformdaki tüm veri sentetiktir, gerçek işlem verisi kullanılmaz. Üretilen müşteri profilleri hem işlem simülasyonunu hem de tespit tarafını besler. Fraud ve AML senaryoları (örneğin structuring, smurfing, hesap ele geçirme ve uykuda kalan hesabın aniden aktifleşmesi) bilinçli olarak akışa enjekte edilir ve bu senaryoların gerçek etiketleri ayrı bir tabloda saklanır. Tespit sistemi bu etiketlere hiçbir şekilde erişemez, böylece sistemin başarısı gerçek ve önyargısız bir şekilde ölçülebilir.

## Kurulum

```bash
# Altyapı servislerini ayağa kaldır (Redpanda, Postgres, Grafana)
docker compose up -d

# Python ortamını hazırla
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Müşteri profillerini üret (tek seferlik)
python scripts/simulator/profile_gen.py

# İşlem üreticiyi başlat
python scripts/simulator/txn_producer.py
```

## Proje Kararları

- **At-least-once ve idempotency:** Mesajların en az bir kez iletilmesi garanti edilir, aynı mesajın tekrar işlenmesi durumunda veri tekrar yazılmaz.
- **Ground-truth izolasyonu:** Enjekte edilen senaryoların türü (normal/zararlı işlem) tespit sistemine hiçbir şekilde sızmaz. Veri erişiminde ayrım veritabanı yetkilendirmesiyle sağlanır.
- **Alert yazım sırası:** Bir alert oluştuğunda önce Postgres'e yazılır ve bu yazımın başarılı olduğu doğrulanır, ardından mesaj kuyruğuna gönderilir. Böylece iki sistem arasında tutarsızlık riski en aza indirilir.

---

# Real Time Money Laundering Detection Platform
(EN 🇬🇧)

> Project Status: Work in progress

This project showcases an end-to-end streaming data platform that generates a synthetic bank transaction stream, consumes it in real time through a Kafka compatible event log (Redpanda), applies rule based AML and fraud detection, processes the results into an analytics layer with dbt incremental models, and monitors everything live with Grafana.

The system evaluates transactions within seconds, catches suspicious behavior patterns, and attaches a clear reason to every alert explaining which rule triggered it and why.

## Tech Stack

| Layer | Technology |
|---|---|
| Messaging | Redpanda |
| Streaming client | Python `confluent-kafka` |
| Schema and validation | `pydantic` |
| Simulation | Python (`faker` and `numpy`) |
| Hot storage | PostgreSQL |
| Transformation | dbt-postgres, incremental models |
| State management | Redis |
| Monitoring | Grafana (using PostgreSQL data source) |
| Notification | Slack and/or SMTP |
| Testing | pytest |
| CI/CD | GitHub Actions |
| Container | Docker Compose |

## Architecture

The platform is built from six main components:

1. The **profile generator** creates thousands of customer profiles. Each profile carries details like home city, income segment, average transaction amount, and active hours.
2. The **transaction producer** uses these profiles to generate a transaction stream through a Poisson process and writes it to the `transactions` topic in Redpanda. With a small probability, realistic fraud and AML scenarios are mixed into the stream as well.
3. The **rule engine (consumer)** consumes transactions, validates them with pydantic, and evaluates rules over per-account sliding time windows. Validated transactions are written to Postgres; when a rule fires, the alert is written to PostgreSQL first and then to the `alerts` topic in Redpanda.
4. The **notifier** listens for alerts and sends notifications through Slack and/or email.
5. **dbt** transforms raw data into cleaned and summarized analytical tables.
6. **Grafana** reads from PostgreSQL and visualizes metrics like transaction volume, alert rate, and detection latency live, making the system's health easy to follow.

Detection logic runs in memory, while analytical calculations run in the SQL layer. This separation keeps fast detection and deeper analysis independent from each other.

## Data Source

All data in the platform is synthetic, no real transaction data is used. The generated customer profiles feed both the transaction simulation and the detection side. Fraud and AML scenarios (such as structuring, smurfing, account takeover, and a dormant account suddenly becoming active) are deliberately injected into the stream, and the true labels for these scenarios are stored in a separate table. The detection system has no access to these labels, so the system's performance can be measured fairly and without bias.

## Initialization

```bash
# Start the infrastructure services (Redpanda, Postgres, Grafana)
docker compose up -d

# Set up the Python environment
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Generate customer profiles (one time only)
python scripts/simulator/profile_gen.py

# Start the transaction producer
python scripts/simulator/txn_producer.py
```

## Project Decisions

- **At-least-once and idempotency:** Message delivery is guaranteed at least once, and if a message gets processed again, it does not get written to the database twice.
- **Ground-truth isolation:** The true labels of injected scenarios (normal/suspicious transaction) never leak into the detection system. Data access authorization is provided by database permissions.
- **Alert write order:** When an alert is created, it is written to PostgreSQL first and this write is confirmed as successful, only then is it sent to the message queue. This keeps the risk of inconsistency between the two systems as low as possible.