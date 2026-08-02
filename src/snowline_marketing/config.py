"""Marketing plugin configuration — env-driven.

Marketing has its OWN database, separate from the platform's and from any
other plugin's. This skeleton phase (spec §13 stage 1) wires the database
and the platform registration only; no intake loop or policy engine reads
these values yet.

Env vars:
  MARKETING_DATABASE_URL   — the marketing store (its own Postgres DB).
  SNOWLINE_PLATFORM_URL    — where the platform runs (the plugin registration
                             endpoint). Shared (unprefixed) across plugins, so
                             one deploy knob points every plugin at the same
                             platform.
  MARKETING_BASE_URL       — where THIS plugin runs, the `base_url` it hands
                             the platform at registration so the gateway can
                             proxy to it.
  MARKETING_GOVERNANCE_URL — where the governance service runs. Policies are
                             governance artifacts (spec §6) and this is the
                             base URL `policy_source.GatewayPolicyProvider`
                             resolves the current policy-set version from.
                             MARKETING-prefixed, unlike SNOWLINE_PLATFORM_URL:
                             the shared unprefixed vars name things every
                             plugin needs (the platform, the heartbeat
                             cadence), whereas a governance base URL is this
                             plugin's own dependency — the house convention is
                             that each service publishes its own
                             `*_BASE_URL`, and inventing an unprefixed
                             SNOWLINE_GOVERNANCE_URL here would collide with
                             governance's own knob for a different meaning.
  MARKETING_BIND_HOST      — the host this service binds to. Defaults to the
                             loopback address (spec §2: "loopback bind") — a
                             deploy that wants tailnet exposure sets this
                             explicitly rather than the service defaulting to
                             a wildcard bind.
  MARKETING_BIND_PORT      — the port this service binds to.
  SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS — how often the registration
                             heartbeat re-asserts this plugin with the
                             platform. Shared (unprefixed) across plugins,
                             like SNOWLINE_PLATFORM_URL — one deploy knob
                             tunes every plugin's cadence.
  MARKETING_ENABLED        — off by default (spec §2: "Off by default").
                             When unset or falsy, the intake and evaluation
                             loops (later phases) must not do anything; the
                             service still starts and serves /health (and,
                             later, read-only audit surfaces) regardless of
                             this flag.
"""

import logging
import math
import os

# Local libpq defaults (unix socket, current OS user, no password) — mirrors
# the platform/plugin house convention. A SEPARATE database: marketing owns
# its own ledgers (once they ship), no other service's tables.
DEFAULT_DATABASE_URL = "postgresql+psycopg:///snowline_marketing"

# Where the platform lives — the POST /plugins registration endpoint.
DEFAULT_PLATFORM_URL = "http://127.0.0.1:8850"

# Where this plugin advertises itself to the platform (the manifest `base_url`).
DEFAULT_BASE_URL = "http://127.0.0.1:8805"

# Where governance lives — the service holding the policy-set artifacts (spec
# §6). Matches governance's OWN default base URL, so a single-host dev box
# needs no configuration at all.
DEFAULT_GOVERNANCE_URL = "http://127.0.0.1:8848"

# Loopback-first bind (spec §2) — a deploy that wants tailnet/LAN exposure
# sets MARKETING_BIND_HOST explicitly; the service never defaults to a
# wildcard.
DEFAULT_BIND_HOST = "127.0.0.1"
DEFAULT_BIND_PORT = 8805

# Registration heartbeat cadence — matches the platform's health-poll default,
# so a platform restart heals in roughly one health round.
DEFAULT_REGISTRATION_HEARTBEAT_SECONDS = 15.0

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def database_url() -> str:
    return os.environ.get("MARKETING_DATABASE_URL", DEFAULT_DATABASE_URL)


def platform_url() -> str:
    return os.environ.get("SNOWLINE_PLATFORM_URL", DEFAULT_PLATFORM_URL).rstrip("/")


def base_url() -> str:
    return os.environ.get("MARKETING_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def governance_url() -> str:
    raw = os.environ.get("MARKETING_GOVERNANCE_URL", DEFAULT_GOVERNANCE_URL)
    return raw.rstrip("/")


def bind_host() -> str:
    return os.environ.get("MARKETING_BIND_HOST", DEFAULT_BIND_HOST)


def bind_port() -> int:
    raw = os.environ.get("MARKETING_BIND_PORT")
    if not raw:
        return DEFAULT_BIND_PORT
    try:
        value = int(raw)
    except ValueError:
        logging.getLogger(__name__).warning(
            "malformed MARKETING_BIND_PORT=%r — using the default %s",
            raw,
            DEFAULT_BIND_PORT,
        )
        return DEFAULT_BIND_PORT
    if not 1 <= value <= 65535:
        # 0 would bind an ephemeral OS-assigned port while MARKETING_BASE_URL
        # keeps advertising the default to the platform — a healthily
        # registered plugin the gateway can't reach; >65535 crashes uvicorn
        # at startup. Same lenient warn-and-fall-back as the other knobs.
        logging.getLogger(__name__).warning(
            "out-of-range MARKETING_BIND_PORT=%r — using the default %s",
            raw,
            DEFAULT_BIND_PORT,
        )
        return DEFAULT_BIND_PORT
    return value


def marketing_enabled() -> bool:
    """Off by default (spec §2): the intake/evaluation loops (later phases)
    must stay dark unless MARKETING_ENABLED is explicitly set to a truthy
    value. Tests pin this off via an autouse fixture (pattern:
    SNOWLINE_SHADOW_TURNS_ENABLED)."""
    return os.environ.get("MARKETING_ENABLED", "").strip().lower() in _TRUTHY


def registration_heartbeat_seconds() -> float:
    """The heartbeat cadence. LENIENT on a malformed/absurd value (warn +
    fall back), unlike a fail-loud config rule: the heartbeat is the
    self-healing mechanism the registration loop exists for, so a typo in
    this shared env var must not kill the loop (a dead heartbeat = a hollow
    gateway after the next platform restart) — and a zero/negative value must
    not hot-loop POSTs."""
    raw = os.environ.get("SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS")
    if raw is None:
        return DEFAULT_REGISTRATION_HEARTBEAT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        logging.getLogger(__name__).warning(
            "malformed SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS=%r — using the "
            "default %ss",
            raw,
            DEFAULT_REGISTRATION_HEARTBEAT_SECONDS,
        )
        return DEFAULT_REGISTRATION_HEARTBEAT_SECONDS
    if not math.isfinite(value):
        # "inf"/"nan" parse as floats and slip past the < 1.0 floor, but
        # anyio.sleep(inf/nan) never returns — a silent dead heartbeat, the
        # exact failure this lenient parse exists to prevent. Treat like
        # malformed input.
        logging.getLogger(__name__).warning(
            "non-finite SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS=%r — using the "
            "default %ss",
            raw,
            DEFAULT_REGISTRATION_HEARTBEAT_SECONDS,
        )
        return DEFAULT_REGISTRATION_HEARTBEAT_SECONDS
    if value < 1.0:
        logging.getLogger(__name__).warning(
            "SNOWLINE_REGISTRATION_HEARTBEAT_SECONDS=%r is below the 1s floor "
            "— clamping (the heartbeat cannot be disabled by env; stop the "
            "plugin instead)",
            raw,
        )
        return 1.0
    return value
