# tests/test_est_gru_ane.py

import pytest
from unittest.mock import patch, MagicMock, ANY
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
    assert estado_gru_ane.get_cod_hito("001") == "ENT001"
    assert estado_gru_ane.get_cod_hito("SPC") == "SAL001"
    assert estado_gru_ane.get_cod_hito("XXX") is None  # Valor inexistente






def test_file_process():
    # Definir un nombre de archivo de prueba
    test_file_name = "test_file.txt"

    # Mockear 'state.MensajeEstado.leer_stat_gruber'
    with patch('src.est_gru_ane.state.MensajeEstado') as MockMensajeEstado, \
         patch('src.est_gru_ane.BmApi') as MockBmApi:

        # Configurar el mock para 'leer_stat_gruber'
        mock_mensaje_estado_instance = MockMensajeEstado.return_value
        mock_mensaje_estado_instance.leer_stat_gruber.return_value = {
            "Lineas": [
                {
                    "Record type ‘Q10’": "Q10",
                    "Consignment number sending depot": "12345",  # n_ref_cor válido
                    "Status code": "001"  # n_status válido
                }
            ]
        }

        # Configurar el mock para 'BmApi' y sus métodos
        mock_bmapi_instance = MockBmApi.return_value
        mock_bmapi_instance.n_consulta.return_value = {
            "contenido": [
                {"ipda": "ipda_value", "cpda": "cpda_value"}  # Respuesta esperada
            ]
        }
        mock_bmapi_instance.post_partida_tracking.return_value = {"status_code": 201}

        # Instanciar la clase que estamos testeando
        estado = EstadoGruAne()

        # Configurar 'files' antes de procesar
        estado.files = {
            test_file_name: {
                "success": False,
                "message": "",
            }
        }

        # Llamar al método que estamos testeando
        estado.file_process(test_file_name)

        # Verificar que 'leer_stat_gruber' fue llamado con el nombre de archivo correcto
        mock_mensaje_estado_instance.leer_stat_gruber.assert_called_once_with(test_file_name)

        # Verificar que los métodos de 'BmApi' fueron llamados
        mock_bmapi_instance.n_consulta.assert_called_once()
        mock_bmapi_instance.post_partida_tracking.assert_called_once_with(
            "ipda_value",
            {
                "codigohito": "ENT001",  # Ajustar según la lógica de get_cod_hito
                "descripciontracking": test_file_name,
                "fechatracking": ANY  # Ignorar valor exacto
            }
        )

        # Verificar los cambios en 'estado.files'
        assert estado.files[test_file_name]["success"] is True
        assert "Creada partida, hito ENT001-cpda_value" in estado.files[test_file_name]["message"]
