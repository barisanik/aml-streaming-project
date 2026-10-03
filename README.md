# Gerçek Zamanlı Kara Para Tespit Platformu
(TR 🇹🇷 | [English below](#real-time-money-laundering-detection-platform))

> Proje Durumu: Devam ediyor.

Bu proje sentetik banka işlem akışı üreten, bu akışı Kafka uyumlu bir event log (Redpanda) üzerinden gerçek zamanlı tüketen, kural tabanlı AML ve fraud tespiti yapan ve ham veriyi dbt incremental modelleriyle analitik katmana hazırlayan uçtan uca bir streaming veri platformudur. Grafana ile operasyonel izleme planlanmıştır.

Sistem işlemleri saniyeler içinde değerlendirir, şüpheli davranış kalıplarını yakalar ve her alert'e hangi kuralın neden tetiklendiğini açıklayan bir gerekçe ekler.

## Kullanılan Teknolojiler

| Katman | Teknoloji | Durum |
|---|---|---|
| Mesaj sistemi | Redpanda | Tamamlandı |
| Streaming client | Python `confluent-kafka` | Tamamlandı |
| Şema ve validasyon | `pydantic` | Tamamlandı |
| Simülasyon | Python (`faker` ve `numpy`) | Tamamlandı |
| Veri depolama | PostgreSQL | Tamamlandı |
| Dönüşüm | dbt-postgres, incremental staging modeli | Tamamlandı |
| Durum yönetimi | Bellek içi sliding window ve Redis | Sliding window iki kural için tamamlandı, Redis planlandı |
| İzleme | Grafana | Planlandı |
| Bildirim | Slack/Discord webhook ve SMTP | Webhook tamamlandı, SMTP planlandı |
| Test | pytest | Tamamlandı |
| CI/CD | GitHub Actions | Tamamlandı |
| Container | Docker Compose altyapı servisleri | Tamamlandı|

## Mimari

Platform altı ana bileşenden oluşur:

1. **Profil üretici**, binlerce müşteri profili oluşturur. Her profil lokasyon, gelir segmenti, ortalama işlem tutarı ve aktif saat aralığı gibi bilgiler taşır.
2. **İşlem üretici (producer)**, bu profilleri kullanarak Poisson süreciyle işlem akışı üretir ve Redpanda'daki `transactions` topic'ine yazar. Belirli bir olasılıkla gerçekçi fraud ve AML senaryoları da bu akışa karıştırılır.
3. **Kural motoru (consumer)**, işlemleri tüketir, pydantic ile doğrular ve hesap bazlı zaman pencereleri üzerinde kuralları değerlendirir. Doğrulanan işlemler Postgres'e yazılır. Bir kural tetiklenirse alert önce Postgres'e, sonra Redpanda'daki `alerts` topic'ine yazılır.
4. **Bildirim servisi (notifier)**, `alerts` topic'ini manuel offset commit ile dinler; Slack veya Discord webhook'una yapılandırılabilir retry ile bildirim gönderir. Webhook hata yanıtının gövdesi loglanır. SMTP ayarı vardır, gönderim henüz uygulanmamıştır.
5. **dbt**, ham işlemleri incremental staging modeline dönüştürür.  
6. **Grafana**, PostgreSQL'den operasyonel metrikleri görselleştirmek üzere planlanmıştır.

Tespit mantığı bellek içinde, analitik hesaplamalar ise SQL katmanında çalışır. Bu ayrım, hızlı tespit ile derinlemesine analizi birbirinden bağımsız tutar.

## Veri Kaynağı

Platformdaki tüm veri sentetiktir, gerçek işlem verisi kullanılmaz. Üretilen müşteri profilleri hem işlem simülasyonunu hem de tespit tarafını besler. Şu an structuring ve smurfing senaryoları akışa enjekte edilir; mule fan-in, hesap ele geçirme ve uykuda kalan hesabın aniden aktifleşmesi sonraki kapsamdadır. Enjekte edilen işlemlerin gerçek etiketleri ayrı `answer_key` tablosunda saklanır. Tespit sistemi bu etiketlere hiçbir şekilde erişemez, böylece sistemin başarısı gerçek ve önyargısız bir şekilde ölçülebilir.

## Varlık İlişki Diyagramı

<img width="781" height="752" alt="AML-ERD" src="https://github.com/user-attachments/assets/ff189e8c-97c7-4600-a3a0-fd35f6b2fe28" />

## Kurulum

```bash
# 1. Environment dosyasının içeriğini düzenle
# '.env-Sample' dosyasının adını '.env' olarak değiştir.
# [POSTGRES_USERNAME] gibi köşeli parantezle belirtilen kullanıcı adlarını ve şifreleri belirle.

# 2. Python sanal ortamını kur
python -m venv venv

# 3. Sanal ortamı aktive et
venv\Scripts\activate (Windows için)
source venv/bin/activate (Mac için)

# 4. Gerekli kütüphanelerin kurulumunu yap
pip install -r requirements.txt

# 5. Altyapı servislerini ayağa kaldır (Redpanda, Postgres, Grafana)
# ÖNEMLİ NOT: Mac bilgisayarınızda halihazırda PostgreSQL kurulu ise Docker'da ayağa kalkacak PostgreSQL'için tanımlanan port'u (docker-compose.yml dosyasında, postgres/ports altında yer alır) değiştirmeyi unutmayın.
docker compose up -d

# 6. Redpanda topic'lerini oluştur
python scripts/redpanda/create_topics.py

# 7. Veri tabanı kurulumu için DDL'i çalıştır
python scripts/postgres/exec_01_init.py

# 8. Veri tabanı kullanıcıları için şifrelerin atanmasını sağla
python scripts/postgres/set_role_passwords.py

# 9. Veri tabanı kurulumunu doğrula (isteğe bağlı)
python scripts/postgres/verify_01_init_ddl.py

# 10. Müşteri profillerini üret (tek seferlik işlem)
python scripts/simulator/profile_gen.py

# 11. dbt paketlerini kur (tek seferlik işlem)
cd [DBT_PROJE_KLASÖRÜ] # cd .\dbt\aml_platform\
dbt deps

# 12. İşlem üreticiyi başlat
python scripts/simulator/txn_producer.py

# 13. İşlem tüketiciyi başlat
python scripts/simulator/txn_consumer.py

# 14. Alert notifier'ı başlat
python scripts/simulator/notifier.py

# 15. dbt scheduler'ı başlat (60 saniyede bir dbt run + test tetikler)
python scripts/dbt/dbt_scheduler.py
```

## Proje Kararları

- **At-least-once ve idempotency:** Mesajların en az bir kez iletilmesi garanti edilir, aynı mesajın tekrar işlenmesi durumunda veri tekrar yazılmaz.
- **Ground-truth izolasyonu:** Enjekte edilen senaryoların türü (normal/zararlı işlem) tespit sistemine hiçbir şekilde sızmaz. Veri erişiminde ayrım veritabanı yetkilendirmesiyle sağlanır.
- **Alert yazım sırası:** Bir alert oluştuğunda önce Postgres'e yazılır ve bu yazımın başarılı olduğu doğrulanır, ardından mesaj kuyruğuna gönderilir. Böylece iki sistem arasında tutarsızlık riski en aza indirilir.
- **Tespit sistemi kuralları:** Structuring ve smurfing event-time sliding window'larda değerlendirilir.
- **Doğrulama:** Birim testleri broker veya veritabanına bağımlı değildir.

---

# Real Time Money Laundering Detection Platform
(EN 🇬🇧)

> Project Status: Work in progress

This project showcases an end-to-end streaming data platform that generates a synthetic bank transaction stream, consumes it in real time through a Kafka compatible event log (Redpanda), applies rule based AML and fraud detection, and prepares raw data for an analytics layer with dbt incremental models. Operational monitoring with Grafana is a planned next step.

The system evaluates transactions within seconds, catches suspicious behavior patterns, and attaches a clear reason to every alert explaining which rule triggered it and why.

## Tech Stack

| Layer | Technology | Status |
|---|---|---|
| Messaging | Redpanda | Done |
| Streaming client | Python `confluent-kafka` | Done |
| Schema and validation | `pydantic` | Done |
| Simulation | Python (`faker` and `numpy`) | Done |
| Data storage | PostgreSQL | Done |
| Transformation | dbt-postgres, incremental staging model | Done |
| State management | In-memory sliding window and Redis | Sliding window is done for two rule, Redis planned |
| Monitoring | Grafana (using PostgreSQL data source) | Planned |
| Notification | Slack/Discord webhook and SMTP | Webhook done, SMTP planned |
| Testing | pytest | Done |
| CI/CD | GitHub Actions | Done |
| Container | Docker Compose infrastructure services | Done|

## Architecture

The platform is built from six main components:

1. The **profile generator** creates thousands of customer profiles. Each profile carries details like home city, income segment, average transaction amount, and active hours.
2. The **transaction producer** uses these profiles to generate a transaction stream through a Poisson process and writes it to the `transactions` topic in Redpanda. With a small probability, realistic fraud and AML scenarios are mixed into the stream as well.
3. The **rule engine (consumer)** consumes transactions, validates them with pydantic, and evaluates rules over per-account sliding time windows. Validated transactions are written to Postgres. When a rule fires, the alert is written to PostgreSQL first and then to the `alerts` topic in Redpanda.
4. The **notifier** consumes the `alerts` topic with manual offset commits and sends Slack or Discord webhook notifications with configurable retries. It logs HTTP error response bodies. SMTP is configured but not implemented.
5. **dbt** transforms raw transactions into an incremental staging model.
6. **Grafana** is planned to visualize operational metrics from PostgreSQL.

Detection logic runs in memory, while analytical calculations run in the SQL layer. This separation keeps fast detection and deeper analysis independent from each other.

## Data Source

All data in the platform is synthetic, no real transaction data is used. The generated customer profiles feed both the transaction simulation and the detection side. Structuring and smurfing scenarios are currently injected into the stream; mule fan-in, account takeover, and dormant-account activation remain planned. True labels for injected transactions are stored in the separate `answer_key` table. The detection system has no access to these labels, so performance can be measured fairly and without bias.

## Entity-Relationship Diagram

<img width="781" height="752" alt="AML-ERD" src="https://github.com/user-attachments/assets/ff189e8c-97c7-4600-a3a0-fd35f6b2fe28" />

## Initialization

```bash
# 1. Modify environment file
# Rename '.env-Sample' file as '.env'.
# Replace placeholders marked by square brackets such as [POSTGRES_USERNAME] with actual password and username.

# 2. Set up the Python environment
python -m venv venv

# 3. Activate virtual environment
venv\Scripts\activate (for Windows)
source venv/bin/activate (for Mac)

# 4. Install necessary libraries
pip install -r requirements.txt

# 5. Start the infrastructure services (Redpanda, Postgres, Grafana)
# WARNING: If you already installed a PostgreSQL on your Mac do not forget to change port of PostgreSQL service on docker-compose.yml (postgres/ports).
docker compose up -d

# 6. Create Redpanda topics
python scripts/redpanda/create_topics.py

# 7. Run DDL script for setup of database
python scripts/postgres/exec_01_init.py

# 8. Execute password set script for database users
python scripts/postgres/set_role_passwords.py

# 9. Verify database initalization (optional)
python scripts/postgres/verify_01_init_ddl.py

# 10. Generate customer profiles (one time only)
python scripts/simulator/profile_gen.py

# 11. install dbt packages (one time only)
cd [DBT_PROJECT_ROOT] # cd .\dbt\aml_platform\
dbt deps

# 12. Start the transaction producer
python scripts/simulator/txn_producer.py

# 13. Start the transaction consumer
python scripts/simulator/txn_consumer.py

# 14. Start the alert notifier
python scripts/simulator/notifier.py

# 15. Start the dbt scheduler (triggers dbt run + test every 60 seconds.)
python scripts/dbt/dbt_scheduler.py
```

## Project Decisions

- **At-least-once and idempotency:** Message delivery is guaranteed at least once, and if a message gets processed again, it does not get written to the database twice.
- **Ground-truth isolation:** The true labels of injected scenarios (normal/suspicious transaction) never leak into the detection system. Data access authorization is provided by database permissions.
- **Alert write order:** When an alert is created, it is written to PostgreSQL first and this write is confirmed as successful, only then is it sent to the message queue. This keeps the risk of inconsistency between the two systems as low as possible.
- **Detection system rules:** Structuring and smurfing run over event-time sliding windows.
- **Verification:** Unit tests do not require a broker or database.
