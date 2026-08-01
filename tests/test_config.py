"""Config parsing — env-driven, off-by-default gate, loopback-first bind."""

from __future__ import annotations

from snowline_marketing import config


def test_marketing_enabled_defaults_off(monkeypatch):
    monkeypatch.delenv("MARKETING_ENABLED", raising=False)
    assert config.marketing_enabled() is False


def test_marketing_enabled_truthy_values(monkeypatch):
    for value in ("1", "true", "True", "yes", "YES", "on"):
        monkeypatch.setenv("MARKETING_ENABLED", value)
        assert config.marketing_enabled() is True, f"{value!r} should be truthy"


def test_marketing_enabled_falsy_values(monkeypatch):
    for value in ("0", "false", "no", "off", "", "garbage"):
        monkeypatch.setenv("MARKETING_ENABLED", value)
        assert config.marketing_enabled() is False, f"{value!r} should be falsy"


def test_bind_host_defaults_to_loopback(monkeypatch):
    monkeypatch.delenv("MARKETING_BIND_HOST", raising=False)
    assert config.bind_host() == "127.0.0.1"


def test_bind_host_overridable(monkeypatch):
    monkeypatch.setenv("MARKETING_BIND_HOST", "0.0.0.0")
    assert config.bind_host() == "0.0.0.0"


def test_bind_port_default(monkeypatch):
    monkeypatch.delenv("MARKETING_BIND_PORT", raising=False)
    assert config.bind_port() == 8805


def test_bind_port_parses_int(monkeypatch):
    monkeypatch.setenv("MARKETING_BIND_PORT", "9000")
    assert config.bind_port() == 9000


def test_bind_port_malformed_falls_back(monkeypatch):
    monkeypatch.setenv("MARKETING_BIND_PORT", "not-a-port")
    assert config.bind_port() == 8805


def test_database_url_default(monkeypatch):
    monkeypatch.delenv("MARKETING_DATABASE_URL", raising=False)
    assert config.database_url() == "postgresql+psycopg:///snowline_marketing"


def test_platform_url_default_and_strips_trailing_slash(monkeypatch):
    monkeypatch.delenv("SNOWLINE_PLATFORM_URL", raising=False)
    assert config.platform_url() == "http://127.0.0.1:8850"
    monkeypatch.setenv("SNOWLINE_PLATFORM_URL", "http://platform.example/")
    assert config.platform_url() == "http://platform.example"


def test_base_url_default_and_strips_trailing_slash(monkeypatch):
    monkeypatch.delenv("MARKETING_BASE_URL", raising=False)
    assert config.base_url() == "http://127.0.0.1:8805"
    monkeypatch.setenv("MARKETING_BASE_URL", "http://marketing.example/")
    assert config.base_url() == "http://marketing.example"


def test_heartbeat_interval_env_is_lenient(monkeypatch):
    # A malformed or hot-looping value in the SHARED env var must not kill the
    # heartbeat (a dead heartbeat = a hollow gateway after the next platform
    # restart) — warn and fall back instead.
    monkeypatch.delenv("SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS", raising=False)
    assert config.registration_heartbeat_seconds() == 15.0
    monkeypatch.setenv("SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS", "15s")
    assert config.registration_heartbeat_seconds() == 15.0  # malformed -> default
    monkeypatch.setenv("SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS", "0")
    assert config.registration_heartbeat_seconds() == 1.0  # floored, no hot loop
    monkeypatch.setenv("SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS", "inf")
    assert config.registration_heartbeat_seconds() == 15.0  # non-finite -> default
    monkeypatch.setenv("SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS", "nan")
    assert config.registration_heartbeat_seconds() == 15.0  # non-finite -> default
    monkeypatch.setenv("SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS", "30")
    assert config.registration_heartbeat_seconds() == 30.0
