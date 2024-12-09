# tests/test_ftp_connect.py

import ftplib
from unittest.mock import patch, MagicMock, mock_open
from utils.ftp_connect import FtpConnection


@patch("utils.ftp_connect.ftplib.FTP")
def test_connect_success(mock_ftp):
    # Mock del objeto FTP
    mock_instance = MagicMock()
    mock_ftp.return_value = mock_instance

    # Crear instancia de FtpConnection
    connection = FtpConnection("test_host", "test_user", "test_password")
    connection.connect()

    # Verificar que los métodos se llamaron correctamente
    mock_instance.connect.assert_called_once_with("test_host", 21)
    mock_instance.login.assert_called_once_with("test_user", "test_password")


@patch("utils.ftp_connect.ftplib.FTP")
def test_connect_failure(mock_ftp):
    # Simular excepción al conectar
    mock_instance = MagicMock()
    mock_ftp.return_value = mock_instance
    mock_instance.connect.side_effect = ftplib.error_perm("Mocked connection error")

    # Crear instancia de FtpConnection
    connection = FtpConnection("test_host", "test_user", "test_password")
    connection.connect()

    # Verificar que se intentó conectar y que se manejó la excepción
    mock_instance.connect.assert_called_once_with("test_host", 21)
    mock_instance.login.assert_not_called()


@patch("utils.ftp_connect.ftplib.FTP")
def test_disconnect_success(mock_ftp):
    # Mock del objeto FTP
    mock_instance = MagicMock()
    mock_ftp.return_value = mock_instance

    # Crear instancia de FtpConnection y conectar
    connection = FtpConnection("test_host", "test_user", "test_password")
    connection.connect()
    connection.disconnect()

    # Verificar que se llamó a quit
    mock_instance.quit.assert_called_once()


@patch("utils.ftp_connect.ftplib.FTP")
def test_disconnect_failure(mock_ftp):
    # Simular excepción al desconectar
    mock_instance = MagicMock()
    mock_ftp.return_value = mock_instance
    mock_instance.quit.side_effect = ftplib.error_perm("Mocked disconnection error")

    # Crear instancia de FtpConnection y conectar
    connection = FtpConnection("test_host", "test_user", "test_password")
    connection.connect()
    connection.disconnect()

    # Verificar que se intentó desconectar
    mock_instance.quit.assert_called_once()


@patch("utils.ftp_connect.open", new_callable=mock_open)
@patch("utils.ftp_connect.ftplib.FTP")
def test_download_file_success(mock_ftp, mock_file):
    # Mock del objeto FTP
    mock_instance = MagicMock()
    mock_ftp.return_value = mock_instance

    # Crear instancia de FtpConnection
    connection = FtpConnection("test_host", "test_user", "test_password")
    connection.connect()

    # Descargar archivo
    connection.download_file("remote/path/file.txt", "local/path/file.txt")

    # Verificar que se llamó a retrbinary
    mock_instance.retrbinary.assert_called_once_with("RETR remote/path/file.txt", mock_file().write)


@patch("utils.ftp_connect.ftplib.FTP")
def test_download_file_no_connection(mock_ftp):
    # Crear instancia de FtpConnection sin conectar
    connection = FtpConnection("test_host", "test_user", "test_password")

    # Intentar descargar un archivo sin conexión
    connection.download_file("remote/path/file.txt", "local/path/file.txt")

    # Verificar que retrbinary no fue llamado
    mock_ftp.return_value.retrbinary.assert_not_called()


@patch("utils.ftp_connect.ftplib.FTP")
def test_change_directory_success(mock_ftp):
    # Mock del objeto FTP
    mock_instance = MagicMock()
    mock_ftp.return_value = mock_instance

    # Crear instancia de FtpConnection y conectar
    connection = FtpConnection("test_host", "test_user", "test_password")
    connection.connect()

    # Cambiar de directorio
    connection.change_directory("/new/directory")

    # Verificar que se llamó a cwd
    mock_instance.cwd.assert_called_once_with("/new/directory")


@patch("utils.ftp_connect.ftplib.FTP")
def test_change_directory_failure(mock_ftp):
    # Mock del objeto FTP
    mock_instance = MagicMock()
    mock_ftp.return_value = mock_instance

    # Simular excepción al cambiar de directorio
    mock_instance.cwd.side_effect = ftplib.error_perm("Mocked directory change error")

    # Crear instancia de FtpConnection y conectar
    connection = FtpConnection("test_host", "test_user", "test_password")
    connection.connect()

    # Intentar cambiar de directorio
    connection.change_directory("/invalid/directory")

    # Verificar que se intentó cambiar de directorio
    mock_instance.cwd.assert_called_once_with("/invalid/directory")
