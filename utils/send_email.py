import os
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv(dotenv_path="../conf/.env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=f"../conf/.env.' + {INTEGRATION_CUST}")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PW")

class EmailSender:

    # TODO The parameters of the __init__ method are hardcoded and should be taken from environment variables. Perhaps
    #  we can configure the class to use default values for parameters that are not passed when instantiating the class.
    # TODO Should be a great idea to send as minimum one email, but as option, varioys separated by commas.
    def __init__(self):
        self.smtp_server = SMTP_SERVER
        self.smtp_port = SMTP_PORT
        self.username = SMTP_USERNAME
        self.password = SMTP_PASSWORD

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