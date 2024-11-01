import pytest
from unittest.mock import patch, MagicMock
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.send_email import EmailSender  # Ajusta 'your_module' al nombre real del archivo

# Prueba exitosa de envío de correo
@patch("smtplib.SMTP_SSL")
def test_send_email_success(mock_smtp_ssl):
    mock_server = MagicMock()
    mock_smtp_ssl.return_value.__enter__.return_value = mock_server

    email_sender = EmailSender()
    result = email_sender.send_email(
        from_addr="from@example.com",
        to_addrs=["to@example.com"],
        subject="Test Subject",
        body="Test Body"
    )

    # Verificar que se intentó enviar el correo
    mock_server.login.assert_called_once_with(email_sender.username, email_sender.password)
    mock_server.sendmail.assert_called_once()
    assert result is True  # El envío debe ser exitoso


# Prueba de error al enviar el correo
@patch("smtplib.SMTP_SSL", side_effect=Exception("Connection error"))
def test_send_email_failure(mock_smtp_ssl):
    email_sender = EmailSender()
    result = email_sender.send_email(
        from_addr="from@example.com",
        to_addrs=["to@example.com"],
        subject="Test Subject",
        body="Test Body"
    )

    # Verificar que el envío falló y que se manejó la excepción
    assert result is False  # Debe devolver False en caso de excepción


# Prueba cuando 'to_addrs' es una cadena en lugar de una lista
@patch("smtplib.SMTP_SSL")
def test_send_email_single_recipient_as_string(mock_smtp_ssl):
    mock_server = MagicMock()
    mock_smtp_ssl.return_value.__enter__.return_value = mock_server

    email_sender = EmailSender()
    result = email_sender.send_email(
        from_addr="from@example.com",
        to_addrs="to@example.com",  # Pasando una cadena en lugar de una lista
        subject="Test Subject",
        body="Test Body"
    )

    # Verificar que el correo se envió correctamente
    mock_server.sendmail.assert_called_once()
    assert result is True  # El envío debe ser exitoso