import logging
import os
import smtplib
import ssl
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Carregar variáveis de ambiente do .env local do projeto.
load_dotenv()


def _env(*keys: str, default=None):
    for key in keys:
        value = os.getenv(key)
        if value is not None and value != "":
            return value
    return default


EMAIL_ADDRESS = _env("EMAIL_ADDRESS", "email_address")
EMAIL_ADDRESS_TO_SENT = _env("EMAIL_ADDRESS_TO_SENT", "email_address_to_sent", "EMAIL_TO")
SMTP_SERVER = _env("SMTP_SERVER", "smtp_server")
SMTP_USERNAME = _env("SMTP_USERNAME", "smtp_username", default=EMAIL_ADDRESS)
SMTP_PASSWORD = _env("SMTP_PASSWORD", "smtp_password")

try:
    SMTP_PORT = int(_env("SMTP_PORT", "smtp_port", default=587))
except (TypeError, ValueError):
    SMTP_PORT = 587

SMTP_USE_TLS = str(_env("SMTP_USE_TLS", "smtp_use_tls", default="true")).lower() != "false"
SMTP_TLS_VERIFY = str(_env("SMTP_TLS_VERIFY", "smtp_tls_verify", default="true")).lower() != "false"

logger = logging.getLogger(__name__)


def send_email_summary(subject: str, body: str) -> None:
    # Send summary email using SMTP configuration from environment variables.
    if not all([EMAIL_ADDRESS, EMAIL_ADDRESS_TO_SENT, SMTP_SERVER, SMTP_USERNAME]):
        logger.warning("Email summary skipped due to incomplete SMTP configuration.")
        return

    message = MIMEText(body, "plain", "utf-8")
    message["Subject"] = subject
    message["From"] = EMAIL_ADDRESS
    recipients = [addr.strip() for addr in EMAIL_ADDRESS_TO_SENT.split(",") if addr.strip()]
    message["To"] = ", ".join(recipients)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
            if SMTP_USE_TLS:
                if SMTP_TLS_VERIFY:
                    context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
                    context.check_hostname = True
                    context.verify_mode = ssl.CERT_REQUIRED
                    context.minimum_version = ssl.TLSVersion.TLSv1_2
                    server.starttls(context=context)
                else:
                    # Compatibility mode for internal SMTP where starttls() works without strict cert validation.
                    server.starttls()

            if SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)

            server.sendmail(EMAIL_ADDRESS, recipients, message.as_string())
            logger.info("Summary email sent to %s", ", ".join(recipients))
    except Exception:
        logger.exception("Failed to send summary email")
