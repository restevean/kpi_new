import pytest
from unittest.mock import patch, MagicMock
from src.est_gru_ane import EstadoGruAne

@pytest.fixture
def estado_gru_ane():
    return EstadoGruAne()


def test_load_dir_files(estado_gru_ane, tmp_path):
    # Simula archivos en el directorio de trabajo
    (tmp_path / "file1.txt").write_text("content")
    (tmp_path / "file2.txt").write_text("content")
    estado_gru_ane.local_work_directory = tmp_path

    files = estado_gru_ane.load_dir_files()
    assert "file1.txt" in files
    assert "file2.txt" in files
    assert files["file1.txt"]["success"] is False


def test_get_cod_hito(estado_gru_ane):
    assert estado_gru_ane.get_cod_hito("SMA") == "ENT001"
    assert estado_gru_ane.get_cod_hito("402") == "SAL001"
    assert estado_gru_ane.get_cod_hito("XXX") is None  # Valor inexistente


@patch("src.est_gru_ane.BmApi")
@patch("src.est_gru_ane.state.MensajeEstado")
def test_file_process(mock_mensaje_estado, mock_bm_api, estado_gru_ane):
    # Configura los mocks
    mock_mensaje_instance = mock_mensaje_estado.return_value
    mock_mensaje_instance.leer_stat_gruber.return_value = {
        "Lineas": [
            {"Record type ‘Q10’": "Q10", "Consignment number sending depot": "12345", "Status code": "001"}
        ]
    }
    mock_bm_instance = mock_bm_api.return_value
    mock_bm_instance.consulta_.return_value = {"contenido": [{"ipda": "123", "cpda": "abc"}]}
    mock_bm_instance.post_partida_tracking.return_value = {"status_code": 201}

    # Ejecuta el méthod de prueba
    estado_gru_ane.file_process("test_file.txt")

    # Verifica los resultados esperados
    file_key = "test_file.txt"
    assert estado_gru_ane.files[file_key]["success"] is True
    assert "Creada partida" in estado_gru_ane.files[file_key]["message"]

# TODO Revise this unit test
"""
@patch("src.est_gru_ane.EmailSender")
@patch("src.est_gru_ane.SftpConnection")
def test_run(mock_sftp, mock_email, estado_gru_ane, tmp_path):
    # Configuración del estado de archivos y de los mocks
    estado_gru_ane.files = {
        "file1.txt": {"success": True, "file_name": "file1.txt", "message": "\nCreada partida, hito -"},
        "file2.txt": {"success": False, "file_name": "file2.txt", "message": "\nNO Creada partida, hito -"},
    }
    estado_gru_ane.local_work_directory = tmp_path
    mock_sftp_instance = mock_sftp.return_value
    mock_email_instance = mock_email.return_value

    # Crear archivos en el directorio temporal para evitar FileNotFoundError
    (tmp_path / "file1.txt").write_text("content")
    (tmp_path / "file2.txt").write_text("content")

    # Ejecuta el method `run`
    estado_gru_ane.run()

    # Verificar que los archivos se movieron correctamente
    success_dir = tmp_path / "success"
    fail_dir = tmp_path / "fail"
    assert (success_dir / "file1.txt").exists()
    assert (fail_dir / "file2.txt").exists()

    # Verifica que se envió el correo con el contenido correcto
    mock_email_instance.send_email.assert_any_call(
        estado_gru_ane.email_from,
        estado_gru_ane.email_to,
        estado_gru_ane.email_subject,
        "\nCreada partida, hito -\nArchivo: file1.txt"
    )
    mock_email_instance.send_email.assert_any_call(
        estado_gru_ane.email_from,
        estado_gru_ane.email_to,
        estado_gru_ane.email_subject,
        "\nNO Creada partida, hito -\nArchivo: file2.txt"
    )
    """
