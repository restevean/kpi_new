# tests/test_est_ane_gru.py

import os
import pytest
from unittest.mock import patch, mock_open
from datetime import datetime
from textwrap import dedent
from src.est_ane_gru import EstadoAneGru
from unittest.mock import ANY, patch


@pytest.fixture
def estado_ane_gru_fixture():
    """
    Fixture para crear una instancia de EstadoAneGru con configuración de variables de entorno
    y mock de BmApi.
    """
    with patch.dict(os.environ, {
        'ENTORNO': 'dev',
        'SFTP_SERVER': 'localhost',
        'SFTP_USER': 'user',
        'SFTP_PW': 'password',
        'SFTP_PORT': '22',
        'SFTP_STAT_IN_DIR': '/remote/in/dir',
        'SFTP_DEV_STAT_IN_DIR': '/remote/dev/in/dir',
        'SFTP_STAT_OUT_DIR': '/remote/out/dir',
        'SFTP_DEV_STAT_OUT_DIR': '/remote/dev/out/dir',
    }):
        # Mockear la clase BmApi en el módulo donde se utiliza (src.est_ane_gru)
        with patch('src.est_ane_gru.BmApi') as MockBmApi:
            mock_bm = MockBmApi.return_value
            # Puedes configurar comportamientos por defecto si es necesario
            # Por ejemplo:
            # mock_bm.post_partida_tracking.return_value = {'status_code': 200, 'contenido': {...}}

            # Instanciar EstadoAneGru sin pasar argumentos
            estado = EstadoAneGru()
            yield estado, mock_bm


def test_init(estado_ane_gru_fixture):
    """
    Test del método __init__ para asegurar que los atributos se inicializan correctamente.
    """
    estado, mock_bm = estado_ane_gru_fixture
    assert estado.entorno == 'dev'
    assert estado.host == 'localhost'
    assert estado.username == 'user'
    assert estado.password == 'password'
    assert estado.port == 22
    assert estado.local_work_directory == "../fixtures"
    assert estado.sftp_stat_in_dir == '/remote/in/dir'
    assert estado.sftp_dev_stat_in_dir == '/remote/dev/in/dir'
    assert estado.sftp_stat_out_dir == '/remote/out/dir'
    assert estado.sftp_dev_stat_out_dir == '/remote/dev/out/dir'
    assert estado.remote_work_out_directory == '/remote/dev/out/dir'
    assert estado.remote_work_in_directory == '/remote/dev/in/dir'
    assert estado.partidas is None
    assert estado.max_itrk is None
    assert estado.bm == mock_bm
    assert estado.query_partidas is not None
    assert estado.conversion_dict is not None


def test_query_repesca_property(estado_ane_gru_fixture):
    """
    Test de la propiedad query_repesca para verificar que genera la consulta SQL correcta.
    """
    estado, mock_bm = estado_ane_gru_fixture
    estado.max_itrk = 1000
    expected_query = dedent(f"""
        SELECT TOP 2000
            trapda.cpda,
            trapda.ipda,
            nrefcor,
            itrk,
            aebtrk.ihit,
            aebhit.chit,
            aebhit.dhit,
            aebtrk.fmod,
            aebtrk.hmod,
            aebtrk.fhit,
            aebtrk.hhit
        FROM
            aebtrk
        INNER JOIN
            trapda
            ON trapda.ipda = aebtrk.creg
            AND aebtrk.dtab = 'trapda'
        INNER JOIN
            aebhit
            ON aebhit.ihit = aebtrk.ihit
        WHERE
            aebtrk.ihit IN (
                0, 302, 469, 493, 507, 508, 511, 512, 513, 523, 524, 526, 527, 530, 541,
                542, 543, 544, 546, 547, 562, 568, 630, 631, 632, 633, 635, 636
            )
            AND trapda.ientcor IN (82861, 232829, 232830, 232831, 232833)
            AND trapda.cpda LIKE 'TIP%'

            --AND ipda = 5215  /* Hay que poner itrk e ipda según las variables acumuladas en el proceso */
            --AND ipda = 5215  /* Hay que poner itrk e ipda según las variables acumuladas en el proceso */
            AND itrk > {estado.max_itrk}
            AND YEAR(fhit) * 100 + MONTH(fhit) > 202409;
    """).strip()

    actual_query = dedent(estado.query_repesca).strip()

    assert actual_query == expected_query





def test_write_txt_file(estado_ane_gru_fixture):
    """
    Test del método write_txt_file para asegurar que escribe el contenido correctamente en el archivo.
    """
    estado, mock_bm = estado_ane_gru_fixture
    cpda = 'CPDA001'
    content = "Test content"
    fixed_datetime = datetime(2023, 1, 1, 12, 30)
    filename = "STATE-CPDA001-202301011230.txt"  # Formato fijo
    expected_path = os.path.join(estado.local_work_directory, filename)

    # Definir una subclase de datetime que siempre devuelve fixed_datetime cuando se llama a now()
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_datetime

    with patch('src.est_ane_gru.os.path.join', return_value=expected_path) as mock_join, \
            patch('src.est_ane_gru.open', mock_open()) as mock_file, \
            patch('src.est_ane_gru.datetime', FixedDateTime):
        # Ejecutar el método
        path = estado.write_txt_file(cpda, content)

        # Verificar que os.path.join fue llamado correctamente
        mock_join.assert_called_once_with(estado.local_work_directory, filename)

        # Verificar que open fue llamado con la ruta correcta y modo 'w'
        mock_file.assert_called_once_with(expected_path, 'w')

        # Verificar que write fue llamado una vez con el contenido correcto
        mock_file().write.assert_called_once_with(content)

        # Verificar que la ruta retornada es la esperada
        assert path == expected_path


def test_upload_file_success(estado_ane_gru_fixture):
    """
    Test del método upload_file para asegurar que sube el archivo correctamente mediante SFTP.
    """
    estado, mock_bm = estado_ane_gru_fixture
    estado.entorno = 'dev'
    local_file_path = "/local/path/file.txt"
    remote_directory = estado.sftp_dev_stat_in_dir
    remote_file_path = f"{remote_directory}/file.txt"

    # Mock paramiko.Transport y SFTPClient
    with patch('paramiko.Transport') as mock_transport_class, \
            patch('paramiko.SFTPClient.from_transport') as mock_sftp_client_class:
        mock_transport = mock_transport_class.return_value
        mock_sftp = mock_sftp_client_class.return_value

        result = estado.upload_file(local_file_path)

        mock_transport_class.assert_called_with((estado.host, estado.port))
        mock_transport.connect.assert_called_with(username=estado.username, password=estado.password)
        mock_sftp.put.assert_called_with(local_file_path, remote_file_path)
        mock_sftp.close.assert_called_once()
        mock_transport.close.assert_called_once()
        assert result is True


def test_upload_file_failure(estado_ane_gru_fixture):
    """
    Test del método upload_file para asegurar que maneja correctamente los fallos en la subida mediante SFTP.
    """
    estado, mock_bm = estado_ane_gru_fixture
    estado.entorno = 'prod'
    local_file_path = "/local/path/file.txt"
    remote_directory = estado.sftp_stat_in_dir
    remote_file_path = f"{remote_directory}/file.txt"

    # Mock paramiko.Transport para lanzar una excepción
    with patch('paramiko.Transport', side_effect=Exception("Connection failed")) as mock_transport_class, \
            patch('paramiko.SFTPClient.from_transport') as mock_sftp_client_class, \
            patch('builtins.print') as mock_print:
        result = estado.upload_file(local_file_path)

        mock_transport_class.assert_called_with((estado.host, estado.port))
        mock_print.assert_called_with(f"Error al subir el archivo {local_file_path} mediante SFTP: Connection failed")
        assert result is False


# tests/test_est_ane_gru.py



@pytest.fixture
def estado_ane_gru_fixture():
    """
    Fixture para crear una instancia de EstadoAneGru con configuración de variables de entorno
    y mock de BmApi.
    """
    with patch.dict(os.environ, {
        'ENTORNO': 'dev',
        'SFTP_SERVER': 'localhost',
        'SFTP_USER': 'user',
        'SFTP_PW': 'password',
        'SFTP_PORT': '22',
        'SFTP_STAT_IN_DIR': '/remote/in/dir',
        'SFTP_DEV_STAT_IN_DIR': '/remote/dev/in/dir',
        'SFTP_STAT_OUT_DIR': '/remote/out/dir',
        'SFTP_DEV_STAT_OUT_DIR': '/remote/dev/out/dir',
    }):
        # Mockear la clase BmApi en el módulo donde se utiliza (src.est_ane_gru)
        with patch('src.est_ane_gru.BmApi') as MockBmApi:
            mock_bm = MockBmApi.return_value
            # Puedes configurar comportamientos por defecto si es necesario
            # Por ejemplo:
            # mock_bm.post_partida_tracking.return_value = {'status_code': 200, 'contenido': {...}}

            # Instanciar EstadoAneGru sin pasar argumentos
            estado = EstadoAneGru()
            yield estado, mock_bm


def test_procesa_partida_success(estado_ane_gru_fixture):
    """
    Test del método procesa_partida para asegurar que procesa una partida correctamente en caso de éxito.
    """
    estado, mock_bm = estado_ane_gru_fixture
    cpda = 'CPDA001'
    q10_lines = [
        {
            "status_code": "ANXE05",
            "date_of_event": "2023-01-01",
            "time_of_event": "12:30:00"
        },
        {
            "status_code": "ANXE07",
            "date_of_event": "2023-01-02",
            "time_of_event": "13:45:00"
        }
    ]

    estado.partidas = {
        cpda: {
            'ipda': 'IPDA001',
            'events': q10_lines
        }
    }

    with patch.object(estado, 'write_txt_file') as mock_write_txt_file, \
            patch.object(estado, 'upload_file', return_value=True) as mock_upload_file, \
            patch('src.est_ane_gru.Fort') as MockFort, \
            patch('src.est_ane_gru.composeQ10') as mock_composeQ10, \
            patch('src.est_ane_gru.os.remove') as mock_remove:  # Mockear os.remove

        # Configurar el mock de Fort
        mock_fort_instance = MockFort.return_value
        mock_fort_instance.header.return_value = "HEADER"
        mock_fort_instance.header_q00.return_value = "HEADER_Q00"
        mock_fort_instance.z_control_record.return_value = "Z_CONTROL"
        mock_fort_instance.cierre.return_value = "CIERRE"

        # Configurar el mock de composeQ10
        mock_composeQ10.side_effect = ["Q10_LINE1", "Q10_LINE2"]

        # Configurar el mock de write_txt_file
        mock_write_txt_file.return_value = "/local/path/file.txt"

        # Ejecutar el método
        txt_file_content = estado.procesa_partida(cpda, q10_lines)

        # Verificar llamadas a Fort
        mock_fort_instance.header.assert_called_once_with("w")
        mock_fort_instance.header_q00.assert_called_once_with("w")
        mock_fort_instance.z_control_record.assert_called_once_with(2)
        mock_fort_instance.cierre.assert_called_once_with("w")

        # Verificar llamadas a composeQ10
        assert mock_composeQ10.call_count == 2
        mock_composeQ10.assert_any_call(
            status_code="COR",  # Según conversion_dict, ANXE05 -> COR
            date_of_event="2023-01-01",
            time_of_event="12:30:00"
        )
        mock_composeQ10.assert_any_call(
            status_code="CRI",  # Según conversion_dict, ANXE07 -> CRI
            date_of_event="2023-01-02",
            time_of_event="13:45:00"
        )

        # Verificar llamada a write_txt_file
        mock_write_txt_file.assert_called_with(cpda, "HEADERHEADER_Q00Q10_LINE1Q10_LINE2Z_CONTROLCIERRE")

        # Verificar llamada a upload_file
        mock_upload_file.assert_called_with("/local/path/file.txt")

        # Verificar que os.remove fue llamado con la ruta correcta
        mock_remove.assert_called_once_with("/local/path/file.txt")

        # Verificar que 'success' se haya actualizado correctamente
        assert estado.partidas[cpda]['success'] == True

        # Verificar contenido retornado
        assert txt_file_content == "HEADERHEADER_Q00Q10_LINE1Q10_LINE2Z_CONTROLCIERRE"


def test_procesa_partida_failure(estado_ane_gru_fixture):
    """
    Test del método procesa_partida para asegurar que maneja correctamente los fallos en la subida.
    """
    estado, mock_bm = estado_ane_gru_fixture
    cpda = 'CPDA002'
    q10_lines = [
        {
            "status_code": "ANXE05",
            "date_of_event": "2023-02-01",
            "time_of_event": "14:00:00"
        }
    ]

    estado.partidas = {
        cpda: {
            'ipda': 'IPDA002',
            'events': q10_lines
        }
    }

    with patch.object(estado, 'write_txt_file') as mock_write_txt_file, \
            patch.object(estado, 'upload_file', return_value=False) as mock_upload_file, \
            patch('src.est_ane_gru.Fort') as MockFort, \
            patch('src.est_ane_gru.composeQ10') as mock_composeQ10, \
            patch('builtins.print') as mock_print:
        mock_fort_instance = MockFort.return_value
        mock_fort_instance.header.return_value = "HEADER"
        mock_fort_instance.header_q00.return_value = "HEADER_Q00"
        mock_fort_instance.z_control_record.return_value = "Z_CONTROL"
        mock_fort_instance.cierre.return_value = "CIERRE"

        mock_composeQ10.return_value = "Q10_LINE1"

        mock_write_txt_file.return_value = "/local/path/file.txt"

        txt_file_content = estado.procesa_partida(cpda, q10_lines)

        # Verificar llamada a write_txt_file
        mock_write_txt_file.assert_called_with(cpda, "HEADERHEADER_Q00Q10_LINE1Z_CONTROLCIERRE")

        # Verificar llamada a upload_file
        mock_upload_file.assert_called_with("/local/path/file.txt")

        # Verificar que os.remove no fue llamado
        with patch('os.remove') as mock_remove:
            estado.procesa_partida(cpda, q10_lines)
            mock_remove.assert_not_called()

        # Verificar mensaje de fallo
        mock_print.assert_called_with(f"Fallo al subir el archivo para cpda {cpda}")

        # Verificar que 'success' se haya actualizado correctamente
        assert estado.partidas[cpda]['success'] == False

        # Verificar contenido retornado
        assert txt_file_content == "HEADERHEADER_Q00Q10_LINE1Z_CONTROLCIERRE"


def test_process_query_response(estado_ane_gru_fixture):
    """
    Test del método process_query_response para asegurar que procesa correctamente la respuesta de la consulta.
    """
    estado, mock_bm = estado_ane_gru_fixture
    query_reply = {
        'status_code': 200,
        'contenido': [
            {'cpda': 'CPDA001', 'ipda': 'IPDA001', 'chit': 'ANXE05', 'fhit': '2023-01-01', 'hhit': '12:30:00'},
            {'cpda': 'CPDA001', 'ipda': 'IPDA001', 'chit': 'ANXE07', 'fhit': '2023-01-02', 'hhit': '13:45:00'},
            {'cpda': 'CPDA002', 'ipda': 'IPDA002', 'chit': 'ANXE06', 'fhit': '2023-01-03', 'hhit': '14:00:00'},
        ]
    }

    with patch.object(estado, 'procesa_partida') as mock_procesa_partida:
        estado.process_query_response(query_reply)

        # Verificar estructura de 'partidas'
        assert estado.partidas == {
            'CPDA001': {
                'ipda': 'IPDA001',
                'events': [
                    {'status_code': 'ANXE05', 'date_of_event': '2023-01-01', 'time_of_event': '12:30:00'},
                    {'status_code': 'ANXE07', 'date_of_event': '2023-01-02', 'time_of_event': '13:45:00'},
                ]
            },
            'CPDA002': {
                'ipda': 'IPDA002',
                'events': [
                    {'status_code': 'ANXE06', 'date_of_event': '2023-01-03', 'time_of_event': '14:00:00'},
                ]
            }
        }

        # Verificar llamadas a procesa_partida
        assert mock_procesa_partida.call_count == 2
        mock_procesa_partida.assert_any_call('CPDA001', [
            {'status_code': 'ANXE05', 'date_of_event': '2023-01-01', 'time_of_event': '12:30:00'},
            {'status_code': 'ANXE07', 'date_of_event': '2023-01-02', 'time_of_event': '13:45:00'}
        ])
        mock_procesa_partida.assert_any_call('CPDA002', [
            {'status_code': 'ANXE06', 'date_of_event': '2023-01-03', 'time_of_event': '14:00:00'}
        ])



def test_actualizar_comunicado_success(estado_ane_gru_fixture):
    estado, mock_bm = estado_ane_gru_fixture
    estado.partidas = {
        'CPDA001': {
            'ipda': 'IPDA001',
            'success': True
        },
        'CPDA002': {
            'ipda': 'IPDA002',
            'success': False
        },
        'CPDA003': {
            'ipda': None,
            'success': True
        },
    }

    # Configurar la respuesta mockeada para post_partida_tracking
    mock_bm.post_partida_tracking.return_value = {
        'status_code': 200,
        'contenido': {"resultado": "éxito"}
    }

    # Usar patch para capturar impresiones y verificar comportamiento
    with patch('builtins.print') as mock_print:
        estado.actualizar_comunicado()

        # Verificar que el método post_partida_tracking se llamó con los argumentos correctos
        mock_bm.post_partida_tracking.assert_called_once_with('IPDA001', {
            "codigohito": "ESTADOCOM",
            "descripciontracking": "ESTADO COMUNICADO",
            "fechatracking": ANY
        })

        # Verificar que post_partida_tracking devolvió la respuesta esperada
        respuesta_mock = mock_bm.post_partida_tracking.return_value
        assert respuesta_mock == {
            'status_code': 200,
            'contenido': {"resultado": "éxito"}
        }

        # Validar el estado del objeto si max_itrk no se actualiza
        assert estado.max_itrk is None  # Confirmamos que max_itrk no cambia.

        # Verificar mensajes impresos
        mock_print.assert_any_call("Comunicado actualizado exitosamente para ipda IPDA001")
        mock_print.assert_any_call("No se encontró 'ipda' para cpda CPDA003")


def test_actualizar_comunicado_error_response(estado_ane_gru_fixture):
    """
    Test del método actualizar_comunicado para asegurar que maneja correctamente respuestas con errores HTTP.
    """
    estado, mock_bm = estado_ane_gru_fixture
    estado.partidas = {
        'CPDA001': {
            'ipda': 'IPDA001',
            'success': True
        },
    }

    # Configurar la respuesta mockeada con error HTTP
    mock_bm.post_partida_tracking.return_value = {
        'status_code': 500,
        'contenido': {"error": "Internal Server Error"}
    }

    with patch('builtins.print') as mock_print:
        estado.actualizar_comunicado()

        # Verificar que post_partida_tracking fue llamado correctamente
        mock_bm.post_partida_tracking.assert_called_once_with('IPDA001', {
            "codigohito": "ESTADOCOM",
            "descripciontracking": "ESTADO COMUNICADO",
            "fechatracking": ANY
        })

        # Verificar que 'max_itrk' no se actualizó
        assert estado.max_itrk is None

        # Verificar mensajes impresos
        mock_print.assert_any_call("Error al actualizar el comunicado para ipda IPDA001: Código de estado 500")


def test_actualizar_comunicado_invalid_response(estado_ane_gru_fixture):
    """
    Test del método actualizar_comunicado para asegurar que maneja respuestas inválidas correctamente.
    """
    estado, mock_bm = estado_ane_gru_fixture
    estado.partidas = {
        'CPDA001': {
            'ipda': 'IPDA001',
            'success': True
        },
    }

    # Configurar la respuesta mockeada con respuesta inválida
    mock_bm.post_partida_tracking.return_value = {
        'invalid_key': 123
    }

    with patch('builtins.print') as mock_print:
        estado.actualizar_comunicado()

        # Verificar que post_partida_tracking fue llamado correctamente
        mock_bm.post_partida_tracking.assert_called_once_with('IPDA001', {
            "codigohito": "ESTADOCOM",
            "descripciontracking": "ESTADO COMUNICADO",
            "fechatracking": ANY
        })

        # Verificar que 'max_itrk' no se actualizó
        assert estado.max_itrk is None

        # Verificar mensajes impresos
        mock_print.assert_any_call("Error al actualizar el comunicado para ipda IPDA001: Respuesta inválida")


def test_run(estado_ane_gru_fixture):
    """
    Test del método run para asegurar que ejecuta el flujo completo correctamente.
    """
    estado, mock_bm = estado_ane_gru_fixture

    # Configurar las respuestas mockeadas para n_consulta con la columna 'itrk'
    first_query_reply = {
        'status_code': 200,
        'contenido': [
            {'cpda': 'CPDA001', 'ipda': 'IPDA001', 'chit': 'ANXE05', 'fhit': '2023-01-01', 'hhit': '12:30:00', 'itrk': 10},
            {'cpda': 'CPDA002', 'ipda': 'IPDA002', 'chit': 'ANXE07', 'fhit': '2023-01-02', 'hhit': '13:45:00', 'itrk': 20},
        ]
    }

    second_query_reply = {
        'status_code': 200,
        'contenido': [
            {'cpda': 'CPDA003', 'ipda': 'IPDA003', 'chit': 'ANXE06', 'fhit': '2023-02-01', 'hhit': '14:00:00', 'itrk': 15},
        ]
    }

    mock_bm.n_consulta.side_effect = [first_query_reply, second_query_reply]

    with patch.object(estado, 'process_query_response') as mock_process_query_response, \
            patch.object(estado, 'actualizar_comunicado') as mock_actualizar_comunicado, \
            patch('pandas.DataFrame.to_markdown') as mock_to_markdown, \
            patch('builtins.print') as mock_print:
        estado.run()

        # Verificar que n_consulta fue llamado dos veces
        assert mock_bm.n_consulta.call_count == 2
        mock_bm.n_consulta.assert_any_call(estado.query_partidas)
        mock_bm.n_consulta.assert_any_call(estado.query_repesca)

        # Verificar que DataFrame.to_markdown fue llamado dos veces
        assert mock_to_markdown.call_count == 2

        # Verificar que process_query_response fue llamado dos veces
        assert mock_process_query_response.call_count == 2
        mock_process_query_response.assert_any_call(first_query_reply)
        mock_process_query_response.assert_any_call(second_query_reply)

        # Verificar que actualizar_comunicado fue llamado dos veces
        assert mock_actualizar_comunicado.call_count == 2

        # Verificar que print fue llamado al menos dos veces para los DataFrames
        assert mock_print.call_count >= 2

        # Verificar que max_itrk se calculó correctamente
        assert estado.max_itrk == 20  # El máximo valor de 'itrk' en las respuestas
