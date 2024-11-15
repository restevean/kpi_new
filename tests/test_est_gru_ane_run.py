import pytest
import os
from unittest.mock import patch, MagicMock
from src.est_gru_ane import EstadoGruAne
import glob


@pytest.fixture
def estado_gru_ane():
    """Fixture to initialise an instance of EstadoGruAne."""
    return EstadoGruAne()

# TODO Fix this test, is tailing
@patch("src.est_gru_ane.SftpConnection")
@patch("src.est_gru_ane.EmailSender")
@patch("src.est_gru_ane.state.MensajeEstado")
@patch("src.est_gru_ane.BmApi")
def test_run(mock_bm_api, mock_mensaje_estado, mock_email_sender, mock_sftp_connection, estado_gru_ane):
    # Mock for EmailSender
    mock_email_instance = mock_email_sender.return_value
    mock_email_instance.send_email = MagicMock()

    # Mock para SftpConnection
    mock_sftp_instance = mock_sftp_connection.return_value
    mock_sftp_instance.sftp = MagicMock()
    mock_sftp_instance.sftp.listdir.return_value = ["testfile.stat"]
    mock_sftp_instance.sftp.stat.return_value.st_mode = 0o100000  # Archivo regular
    mock_sftp_instance.sftp.get = MagicMock()
    mock_sftp_instance.sftp.put = MagicMock()
    mock_sftp_instance.sftp.remove = MagicMock()

    # Mock for leer_stat_gruber
    mock_mensaje_estado_instance = mock_mensaje_estado.return_value
    mock_mensaje_estado_instance.leer_stat_gruber.return_value = {
        "Lineas": [{"Record type ‘Q10’": "Q10", "Consignment number sending depot": "12345", "Status code": "001"}]
    }

    # Mock para BmApi
    mock_bm_api_instance = mock_bm_api.return_value
    mock_bm_api_instance.consulta_.return_value = {
        "contenido": [{"ipda": "IPDA123", "cpda": "CPDA123"}]
    }
    mock_bm_api_instance.post_partida_tracking.return_value = {"status_code": 201}

    # Create the test file in the local directory
    local_file = os.path.join(estado_gru_ane.local_work_directory, "testfile.stat")
    os.makedirs(estado_gru_ane.local_work_directory, exist_ok=True)
    with open(local_file, "w") as f:
        f.write("contenido de prueba")

    try:
        # Execute the `run` method
        estado_gru_ane.run()

        # Verify that email_body and email_to were configured.
        assert estado_gru_ane.email_body is not None, "email_body debería estar configurado"
        assert estado_gru_ane.email_to is not None, "email_to debería estar configurado"

        # Verify that the mail was sent
        mock_email_instance.send_email.assert_called()

        # Verify that SFTP was called to move files to remote directory
        mock_sftp_instance.sftp.put.assert_called()
        mock_sftp_instance.sftp.remove.assert_called()

        # Debug target directory
        print("Archivos en el directorio success:", glob.glob(os.path.join(estado_gru_ane.local_work_directory, "success", "*")))
        print("Archivos en el directorio fail:", glob.glob(os.path.join(estado_gru_ane.local_work_directory, "fail", "*")))

        # Verify that at least one file was moved to the `success` directory.
        success_files = glob.glob(os.path.join(estado_gru_ane.local_work_directory, "success", "*"))
        assert len(success_files) > 0, "El archivo no se movió correctamente a la carpeta 'success'"

    finally:
        # Clean up generated files
        if os.path.exists(local_file):
            os.remove(local_file)
        for file in glob.glob(os.path.join(estado_gru_ane.local_work_directory, "success", "*")):
            os.remove(file)
        for file in glob.glob(os.path.join(estado_gru_ane.local_work_directory, "fail", "*")):
            os.remove(file)
