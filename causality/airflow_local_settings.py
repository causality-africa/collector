import logging
import os

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration


sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN"),
    send_default_pii=False,
    integrations=[
        LoggingIntegration(
            level=logging.WARNING,
            event_level=logging.WARNING,
        )
    ],
)
