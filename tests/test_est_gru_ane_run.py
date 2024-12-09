import pytest
import os
from src.est_gru_ane import EstadoGruAne
import glob
import tempfile

@pytest.fixture
def estado_gru_ane():
    """Fixture para inicializar una instancia de EstadoGruAne."""
    return EstadoGruAne()


def test_run(mocker, estado_gru_ane):
    """
    Prueba unitaria para el methodo run() de la clase EstadoGruAne.
    Verifica que el methodo procesa correctamente los archivos,
    interactúa con los servicios externos y maneja adecuadamente
    los estados de éxito y fallo.
    """
    # Configuración de los mocks usando pytest-mock (mocker)

    # Mock para EmailSender
    mock_email_sender = mocker.patch("src.est_gru_ane.EmailSender")
    mock_email_instance = mock_email_sender.return_value
    mock_email_instance.send_email = mocker.MagicMock()

    # Mock para SftpConnection
    mock_sftp_connection = mocker.patch("src.est_gru_ane.SftpConnection")
    mock_sftp_instance = mock_sftp_connection.return_value
    mock_sftp_instance.connect = mocker.MagicMock()
    mock_sftp_instance.disconnect = mocker.MagicMock()
    mock_sftp_instance.sftp = mocker.MagicMock()
    mock_sftp_instance.sftp.listdir.return_value = ["testfile.stat"]
    mock_sftp_instance.sftp.stat.return_value.st_mode = 0o100000  # Archivo regular
    mock_sftp_instance.sftp.get = mocker.MagicMock()
    mock_sftp_instance.sftp.put = mocker.MagicMock()
    mock_sftp_instance.sftp.remove = mocker.MagicMock()

    # Mock para MensajeEstado
    mock_mensaje_estado = mocker.patch("src.est_gru_ane.state.MensajeEstado")
    mock_mensaje_estado_instance = mock_mensaje_estado.return_value
    mock_mensaje_estado_instance.leer_stat_gruber.return_value = {
        "Lineas": [
            {
                "Record type ‘Q10’": "Q10",
                "Consignment number sending depot": "12345",
                "Status code": "001"
            }
        ]
    }

    # Mock para BmApi
    mock_bm_api = mocker.patch("src.est_gru_ane.BmApi")
    mock_bm_api_instance = mock_bm_api.return_value
    mock_bm_api_instance.consulta_.return_value = {
        "contenido": [{"ipda": "IPDA123", "cpda": "CPDA123"}]
    }
    mock_bm_api_instance.post_partida_tracking.return_value = mocker.MagicMock(status_code=201)

    # Uso de un directorio temporal para el trabajo
    with tempfile.TemporaryDirectory() as temp_dir:
        # Actualizar el directorio de trabajo local de estado_gru_ane
        estado_gru_ane.local_work_directory = temp_dir

        # Crear el archivo de prueba en el directorio temporal
        test_file_name = "testfile.stat"
        local_file = os.path.join(estado_gru_ane.local_work_directory, test_file_name)
        with open(local_file, "w") as f:
            f.write("contenido de prueba")

        # Añadir el archivo a self.files
        estado_gru_ane.files = {
            test_file_name: {"success": None, "message": None}
        }

        # Ejecutar el méthodo run()
        estado_gru_ane.run()

        # Verificaciones

        # Verificar que connect y disconnect fueron llamados
        mock_sftp_instance.connect.assert_called_once()
        mock_sftp_instance.disconnect.assert_called_once()

        # Verificar que email_body y email_to fueron configurados
        assert estado_gru_ane.email_body is not None, "email_body debería estar configurado"
        assert estado_gru_ane.email_to is not None, "email_to debería estar configurado"

        # Verificar que el correo fue enviado con los parámetros correctos
        mock_email_instance.send_email.assert_called_once_with(
            estado_gru_ane.email_from,
            estado_gru_ane.email_to,
            estado_gru_ane.email_subject,
            estado_gru_ane.email_body
        )

        # Verificar que SFTP.put y SFTP.remove fueron llamados con los parámetros correctos
        success_status = estado_gru_ane.files[test_file_name]["success"]
        remote_dir = f"{estado_gru_ane.remote_work_in_directory}/OK" if success_status \
            else f"{estado_gru_ane.remote_work_in_directory}/ERROR"
        remote_path = f"{remote_dir}/{test_file_name}"

        mock_sftp_instance.sftp.put.assert_called_once_with(local_file, remote_path)
        mock_sftp_instance.sftp.remove.assert_called_once_with(
            f"{estado_gru_ane.remote_work_out_directory}/{test_file_name}"
        )

        # Verificar que el archivo fue movido al directorio correcto ('success' o 'fail')
        target_dir = os.path.join(
            estado_gru_ane.local_work_directory,
            "success" if success_status else "fail"
        )
        moved_file = os.path.join(target_dir, test_file_name)
        assert os.path.exists(moved_file), f"El archivo no se movió correctamente a la carpeta '{target_dir}'"

        # Depuración opcional: listar archivos en directorios success y fail
        success_files = glob.glob(os.path.join(estado_gru_ane.local_work_directory, "success", "*"))
        fail_files = glob.glob(os.path.join(estado_gru_ane.local_work_directory, "fail", "*"))
        print("Archivos en el directorio 'success':", success_files)
        print("Archivos en el directorio 'fail':", fail_files)
