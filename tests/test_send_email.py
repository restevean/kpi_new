import os
import pytest
from unittest.mock import patch, MagicMock

# Configura las variables de entorno necesarias antes de la importación
os.environ["SMTP_SERVER"] = "smtp.test.com"
os.environ["SMTP_PORT"] = "465"
os.environ["SMTP_USERNAME"] = "test_user"
os.environ["SMTP_PW"] = "test_password"

from utils.send_email import EmailSender  # Importa después de configurar el entorno


@pytest.fixture
def setup_env():
    # Fixture para limpiar las variables de entorno después de cada prueba
    yield  # Permite la ejecución del código de prueba

    # Limpia las variables de entorno después de la prueba
    os.environ.pop("SMTP_SERVER", None)
    os.environ.pop("SMTP_PORT", None)
    os.environ.pop("SMTP_USERNAME", None)
    os.environ.pop("SMTP_PW", None)


@patch("smtplib.SMTP_SSL")
def test_send_email_success(mock_smtp, setup_env):
    # Simula el servidor SMTP
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server

    email_sender = EmailSender()
    result = email_sender.send_email(
        from_addr="sender@example.com",
        to_addrs=["recipient@example.com"],
        subject="Test Email",
        body="This is a test email."
    )

    # Verifica que el login y el envío de correo se realizaron correctamente
    mock_server.login.assert_called_once_with("test_user", "test_password")
    mock_server.sendmail.assert_called_once()  # Verifica que sendmail se llama una vez
    assert result is True


@patch("smtplib.SMTP_SSL", side_effect=Exception("Connection error"))
def test_send_email_failure(mock_smtp, setup_env):
    # Simula un fallo en la conexión SMTP
    email_sender = EmailSender()
    result = email_sender.send_email(
        from_addr="sender@example.com",
        to_addrs=["recipient@example.com"],
        subject="Test Email",
        body="This is a test email."
    )

    # Verifica que el méthod devuelve False al fallar el envío
    assert result is False


@patch("smtplib.SMTP_SSL")
def test_send_email_with_string_recipient(mock_smtp, setup_env):
    # Verifica que el méthod maneja un destinatario único en formato string
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server

    email_sender = EmailSender()
    result = email_sender.send_email(
        from_addr="sender@example.com",
        to_addrs="recipient@example.com",  # Destinatario único como string
        subject="Test Email",
        body="This is a test email."
    )

    # Verifica que el correo fue enviado y el destinatario fue convertido a lista
    mock_server.sendmail.assert_called_once()  # Verifica que sendmail se llama una vez
    assert result is True


@patch("smtplib.SMTP_SSL")
def test_send_email_multiple_recipients(mock_smtp, setup_env):
    # Verifica que el méthod maneja múltiples destinatarios correctamente
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server

    email_sender = EmailSender()
    result = email_sender.send_email(
        from_addr="sender@example.com",
        to_addrs=["recipient1@example.com", "recipient2@example.com"],
        subject="Test Email",
        body="This is a test email."
    )

    # Verifica que el correo fue enviado a múltiples destinatarios
    mock_server.sendmail.assert_called_once()  # Verifica que sendmail se llama una vez
    assert result is True
