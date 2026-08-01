"""`python -m snowline_marketing` — serve on the configured bind.

The blessed way to run the service: it reads MARKETING_BIND_HOST /
MARKETING_BIND_PORT (loopback-first defaults, spec §2) so the bind knobs and
the advertised MARKETING_BASE_URL live in one config surface instead of
drifting apart in a hand-typed uvicorn command.
"""

import uvicorn

from snowline_marketing import config


def main() -> None:
    uvicorn.run(
        "snowline_marketing.app:app",
        host=config.bind_host(),
        port=config.bind_port(),
    )


if __name__ == "__main__":
    main()
