# Credentials Matrix

| System | Variable | Format | Notes |
|---|---|---|---|
| Postgres | POSTGRES_USER | text | Superuser name; created by the postgres container, used by bootstrap scripts to connect as admin |
| Postgres | POSTGRES_PASSWORD | text | Superuser password |
| Postgres | APP_PRODUCER_DB_PASSWORD | text | Password for the `app_producer` role (set by set_role_passwords.py) |
| Postgres | APP_CONSUMER_DB_PASSWORD | text | Password for the `app_consumer` role |
| Postgres | APP_NOTIFIER_DB_PASSWORD | text | Password for the `app_notifier` role |
| Postgres | APP_DBT_DB_PASSWORD | text | Password for the `app_dbt` role |
| Postgres | APP_GRAFANA_DB_PASSWORD | text | Password for the `app_grafana` role. Also passed to the Grafana container for the provisioned datasource |
| Slack/Discord | NOTIFIER_WEBHOOK_URL | absolute URL | Read by notifier.py and by the Grafana alert contact point. For Discord in Grafana add `/slack` to the URL |
| Grafana | admin / admin | text | Grafana default login, changed on first login. Not stored in `.env` |
| SMTP | SMTP_HOST / SMTP_USER / SMTP_PASS | text | Planned, not read by code yet |
