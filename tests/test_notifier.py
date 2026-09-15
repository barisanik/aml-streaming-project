import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts", "simulator"))

from notifier import (
    Alert,
    MessageResult,
    build_webhook_payload,
    format_alert_message,
    get_optional_env,
    main,
    process_alert_message,
    send_webhook_notification,
)


def build_alert() -> Alert:
    return Alert(
        alert_id="11111111-1111-1111-1111-111111111111",
        transaction_id="22222222-2222-2222-2222-222222222222",
        account_id="33333333-3333-3333-3333-333333333333",
        rule_id="R-001",
        rule_name="structuring",
        severity="high",
        window_summary={"transaction_count": 3, "total_amount": "27000.00"},
        event_time=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        alert_time=datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
        detection_latency_ms=1000,
    )


def webhook_config(webhook_type="slack", max_attempts=3, backoff_seconds=0):
    return {
        "type": webhook_type,
        "timeout_seconds": 10,
        "retry": {
            "max_attempts": max_attempts,
            "backoff_seconds": backoff_seconds,
        },
    }


def test_get_optional_env_returns_configured_value():
    with patch.dict("notifier.os.environ", {"TEST_WEBHOOK_URL": "https://example.test"}):
        assert get_optional_env("TEST_WEBHOOK_URL") == "https://example.test"


def test_get_optional_env_returns_none_for_missing_variable():
    with patch.dict("notifier.os.environ", {}, clear=True):
        assert get_optional_env("TEST_WEBHOOK_URL") is None


def test_format_alert_message_contains_required_fields():
    alert = build_alert()

    message = format_alert_message(alert)

    assert "Rule: structuring" in message
    assert "Severity: high" in message
    assert f"Account ID: {alert.account_id}" in message
    assert f"Transaction ID: {alert.transaction_id}" in message
    assert f"Alert time: {alert.alert_time.isoformat()}" in message


def test_build_webhook_payload_uses_slack_text_field():
    payload = build_webhook_payload(build_alert(), "slack")

    assert set(payload) == {"text"}


def test_build_webhook_payload_uses_discord_content_field():
    payload = build_webhook_payload(build_alert(), "discord")

    assert set(payload) == {"content"}


def test_send_webhook_notification_returns_true_for_success():
    response = MagicMock(status_code=204)

    with patch("notifier.requests.post", return_value=response) as mock_post:
        delivered = send_webhook_notification(
            build_alert(), webhook_config(), "https://example.test/webhook"
        )

    assert delivered is True
    mock_post.assert_called_once()


def test_send_webhook_notification_logs_response_body_for_http_failure():
    response = MagicMock(status_code=500, text="webhook unavailable")

    with (
        patch("notifier.requests.post", return_value=response),
        patch("notifier.logging.error") as mock_error,
    ):
        delivered = send_webhook_notification(
            build_alert(),
            webhook_config(max_attempts=1),
            "https://example.test/webhook",
        )

    assert delivered is False
    assert "webhook unavailable" in mock_error.call_args.args


def test_send_webhook_notification_retries_until_success():
    failure = MagicMock(status_code=503, text="try again")
    success = MagicMock(status_code=200, text="ok")

    with patch("notifier.requests.post", side_effect=[failure, success]) as mock_post:
        delivered = send_webhook_notification(
            build_alert(), webhook_config(), "https://example.test/webhook"
        )

    assert delivered is True
    assert mock_post.call_count == 2


def test_process_alert_message_skips_invalid_alert_without_webhook_call():
    with patch("notifier.requests.post") as mock_post:
        result = process_alert_message(
            b'{"alert_id": "missing-required-fields"}',
            webhook_config(),
            "https://example.test/webhook",
            {"enabled": False},
        )

    assert result == MessageResult.VALIDATION_FAILED
    mock_post.assert_not_called()


def test_process_alert_message_delivers_valid_alert():
    alert_raw = json.dumps(build_alert().model_dump(mode="json")).encode("utf-8")
    response = MagicMock(status_code=200, text="ok")

    with patch("notifier.requests.post", return_value=response):
        result = process_alert_message(
            alert_raw,
            webhook_config("discord"),
            "https://example.test/webhook",
            {"enabled": False},
        )

    assert result == MessageResult.DELIVERED


def test_process_alert_message_skips_webhook_when_url_is_empty():
    alert_raw = json.dumps(build_alert().model_dump(mode="json")).encode("utf-8")

    with patch("notifier.requests.post") as mock_post:
        result = process_alert_message(
            alert_raw,
            webhook_config(),
            None,
            {"enabled": False},
        )

    assert result == MessageResult.DELIVERED
    mock_post.assert_not_called()


def test_main_commits_and_continues_after_delivery_failure():
    alert_raw = json.dumps(build_alert().model_dump(mode="json")).encode("utf-8")
    msg = MagicMock()
    msg.value.return_value = alert_raw
    msg.error.return_value = None
    msg.topic.return_value = "alerts"
    msg.partition.return_value = 0
    msg.offset.return_value = 10

    consumer = MagicMock()
    consumer.poll.side_effect = [msg, KeyboardInterrupt]
    config = {
        "webhook": {"url_env_var": "TEST_WEBHOOK_URL"},
        "smtp": {"enabled": False},
    }

    with (
        patch("notifier.load_config", return_value=config),
        patch("notifier.get_optional_env", return_value="https://example.test"),
        patch("notifier.build_consumer", return_value=consumer),
        patch(
            "notifier.process_alert_message",
            return_value=MessageResult.DELIVERY_FAILED,
        ),
        patch("notifier.logging.critical") as mock_critical,
    ):
        main()

    consumer.commit.assert_called_once_with(message=msg, asynchronous=False)
    consumer.close.assert_called_once()
    assert build_alert().alert_id in mock_critical.call_args.args
