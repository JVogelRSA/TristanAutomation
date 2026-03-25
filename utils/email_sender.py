import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / '.env')

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")


def send_report_email(subject, body_text, recipient, attachments=None):
    """
    Send an email with optional file attachments.
    attachments: list of (filename, bytes_data) tuples
    """
    if not recipient:
        print("No recipient specified.")
        return

    if not SMTP_USERNAME or not SMTP_PASSWORD:
        print("Error: SMTP credentials not configured.")
        return

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = SMTP_USERNAME
    msg['To'] = recipient

    msg.attach(MIMEText(body_text, 'plain'))

    if attachments:
        for filename, data in attachments:
            part = MIMEApplication(data, Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)

    try:
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(msg)
        print(f"Email sent to {recipient}!")
    except Exception as e:
        print(f"Error sending email: {e}")
