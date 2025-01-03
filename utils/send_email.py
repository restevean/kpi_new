import os
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

base_dir = Path(__file__).parent
base_dir= base_dir.parent
load_dotenv(dotenv_path=base_dir/ "conf" /".env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=base_dir/ "conf" / f".env.{INTEGRATION_CUST}{ENTORNO}")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PW")

class EmailSender:

    def __init__(self, smtp_server=None, smtp_port=None, username=None, password=None):
        self.smtp_server = smtp_server or os.getenv("SMTP_SERVER")
        self.smtp_port = smtp_port or os.getenv("SMTP_PORT")
        self.username = username or os.getenv("SMTP_USERNAME")
        self.password = password or os.getenv("SMTP_PW")


    def send_email(self, from_addr: str, to_addrs: list[str], subject: str, body: str):
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]

        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = ", ".join(to_addrs)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.username, self.password)  # Iniciar sesión en el servidor
                server.sendmail(from_addr, to_addrs, msg.as_string())  # Enviar correo
            print(f"Correo enviado con éxito a {to_addrs}.")
            return True
        except Exception as e:
            print(f"Error al enviar correo a {to_addrs}: {e}")
            return False


if __name__ == "__main__":
    email_sender = EmailSender()
    email_sender.send_email(
        from_addr="integraciones@anexalogistica.com",
        to_addrs=["restevean@protonmail.com"],
        subject="Prueba de correo",
        body="Este es un correo de prueba."
    )