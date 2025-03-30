import sentry_sdk


def send_error_to_sentry(context):
    """Capture and send exception to Sentry."""
    sentry_sdk.capture_exception(context["exception"])
