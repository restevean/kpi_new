import os
import pytest
from unittest.mock import patch, MagicMock
from utils.send_email import EmailSender
from email.mime.text import MIMEText

@pytest.fixture
def setup_env():
    """Fixture que asegura que las variables de entorno necesarias estén definidas."""
    with patch.dict(os.environ, {
        "SMTP_USERNAME": "test_user",
        "SMTP_PW": "test_password",
        "SMTP_SERVER": "smtp.testserver.com",
        "SMTP_PORT": "465"
    }):
        yield


@patch("smtplib.SMTP_SSL")
def test_send_email_success(mock_smtp, setup_env):
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server

    email_sender = EmailSender(
        smtp_server="smtp.testserver.com",
        smtp_port="465",
        username="test_user",
        password="test_password"
    )
    result = email_sender.send_email(
        from_addr="sender@example.com",
        to_addrs=["recipient@example.com"],
        subject="Test Email",
        body="This is a test email."
    )

    mock_server.login.assert_called_once_with("test_user", "test_password")

    # Crear un mensaje MIME para validación parcial
    msg = MIMEText("This is a test email.")
    msg["Subject"] = "Test Email"
    msg["From"] = "sender@example.com"
    msg["To"] = "recipient@example.com"

    # Verificar que el mensaje enviado contiene las partes esenciales
    actual_call_args = mock_server.sendmail.call_args[0]
    assert actual_call_args[0] == "sender@example.com"
    assert actual_call_args[1] == ["recipient@example.com"]
    assert "This is a test email." in actual_call_args[2]


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

    # Verifica que el method devuelve False al fallar el envío
    assert result is False


@patch("smtplib.SMTP_SSL")
def test_send_email_with_string_recipient(mock_smtp, setup_env):
    # Verifica que el method maneja un destinatario único en formato string
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
    assert result is True  # Ajustado para verificar un booleano en lugar de una cadena


@patch("smtplib.SMTP_SSL")
def test_send_email_multiple_recipients(mock_smtp, setup_env):
    # Verifica que el method maneja múltiples destinatarios correctamente
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
    assert result is True  # Ajustado para verificar un booleano en lugar de una cadena
