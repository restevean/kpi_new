import pytest
from unittest.mock import patch, MagicMock
from utils.sftp_connect import SftpConnection  # Ajusta la ruta de importación si es necesario


@pytest.fixture
def sftp_connection():
    """Fixture que crea una instancia de SftpConnection."""
    return SftpConnection()


@patch("utils.sftp_connect.paramiko.Transport")
def test_connect(mock_transport, sftp_connection):
    # Configura el mock para simular la conexión de transporte
    mock_transport_instance = mock_transport.return_value
    mock_transport_instance.connect = MagicMock()
    mock_sftp_client = MagicMock()
    with patch("utils.sftp_connect.paramiko.SFTPClient.from_transport", return_value=mock_sftp_client):
        # Llama al method `connect`
        sftp_connection.connect()

        # Verifica que Transport se haya inicializado con el host y el puerto correctos
        mock_transport.assert_called_once_with((sftp_connection.host, sftp_connection.port))

        # Verifica que se haya llamado a `connect` con las credenciales correctas
        mock_transport_instance.connect.assert_called_once_with(
            username=sftp_connection.username, password=sftp_connection.password
        )

        # Verifica que SFTPClient se haya creado a partir de `Transport`
        assert sftp_connection.sftp == mock_sftp_client
        print("Conexión establecida")


@patch("utils.sftp_connect.paramiko.Transport")
def test_disconnect(mock_transport, sftp_connection):
    # Configura el mock para la desconexión
    mock_transport_instance = mock_transport.return_value
    sftp_mock = MagicMock()
    sftp_connection.sftp = sftp_mock
    sftp_connection.transport = mock_transport_instance

    # Llama al method `disconnect`
    sftp_connection.disconnect()

    # Verifica que se hayan cerrado tanto el SFTPClient como el Transport
    sftp_mock.close.assert_called_once()
    mock_transport_instance.close.assert_called_once()
    print("Conexión cerrada")


@patch("utils.sftp_connect.paramiko.Transport")
def test_connect_disconnect_integration(mock_transport, sftp_connection):
    # Prueba de conexión y desconexión
    mock_transport_instance = mock_transport.return_value
    mock_transport_instance.connect = MagicMock()
    mock_sftp_client = MagicMock()

    with patch("utils.sftp_connect.paramiko.SFTPClient.from_transport", return_value=mock_sftp_client):
        # Conecta y luego desconecta
        sftp_connection.connect()
        sftp_connection.disconnect()

        # Verifica que `connect` y `close` se llamen en orden
        mock_transport.assert_called_once_with((sftp_connection.host, sftp_connection.port))
        mock_transport_instance.connect.assert_called_once_with(
            username=sftp_connection.username, password=sftp_connection.password
        )
        mock_sftp_client.close.assert_called_once()
        mock_transport_instance.close.assert_called_once()
