import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class EmailSender:

    # TODO The parameters of the __ini__ method are hardcoded and should be taken from environment variables.
    def __init__(self):
        self.smtp_server = 'smtp.gmail.com'
        self.smtp_port = 465
        self.username = "integraciones@anexalogistica.com"
        self.password = "i5486ntegraciones"

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