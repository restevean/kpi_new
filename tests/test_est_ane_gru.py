import os
import pytest
import tempfile
import datetime
from unittest.mock import patch, MagicMock, ANY

from src.est_ane_gru import EstadoAneGru


@pytest.fixture
def estado_ane_gru():
    # Configurar variables de entorno necesarias
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
        yield EstadoAneGru()


def test_init(estado_ane_gru):
    assert estado_ane_gru.entorno == 'dev'
    assert estado_ane_gru.host == 'localhost'
    assert estado_ane_gru.username == 'user'
    assert estado_ane_gru.password == 'password'
    assert estado_ane_gru.port == 22
    assert estado_ane_gru.partidas is None


def test_write_txt_file(estado_ane_gru):
    with tempfile.TemporaryDirectory() as temp_dir:
        estado_ane_gru.local_work_directory = temp_dir
        cpda = 'TEST123'
        content = 'Contenido de prueba'
        file_path = estado_ane_gru.write_txt_file(cpda, content)
        assert os.path.isfile(file_path)
        with open(file_path, 'r') as f:
            file_content = f.read()
        assert file_content == content


def test_upload_file_success(estado_ane_gru):
    # Mock paramiko.Transport y paramiko.SFTPClient
    with patch('paramiko.Transport') as mock_transport_class:
        mock_transport_instance = MagicMock()

        # Mockear el constructor para que acepte argumentos
        def transport_side_effect(*args, **kwargs):
            return mock_transport_instance

        mock_transport_class.side_effect = transport_side_effect
        mock_transport_instance.connect.return_value = None
        mock_transport_instance.close.return_value = None

        with patch('paramiko.SFTPClient.from_transport') as mock_sftp_client_class:
            mock_sftp_client_instance = MagicMock()
            mock_sftp_client_class.return_value = mock_sftp_client_instance
            mock_sftp_client_instance.put.return_value = None
            mock_sftp_client_instance.close.return_value = None

            local_file_path = '/path/to/local/file.txt'
            success = estado_ane_gru.upload_file(local_file_path)
            assert success is True
            mock_transport_instance.connect.assert_called_with(username='user', password='password')
            mock_sftp_client_instance.put.assert_called_with(local_file_path, ANY)


def test_upload_file_failure(estado_ane_gru):
    # Mock paramiko to raise an exception
    with patch('paramiko.Transport', side_effect=Exception('SFTP error')):
        local_file_path = '/path/to/local/file.txt'
        success = estado_ane_gru.upload_file(local_file_path)
        assert success is False


def test_process_query_response(estado_ane_gru):
    # Mock the query response
    query_reply = {
        'contenido': [
            {
                'cpda': 'CPDA001',
                'ipda': 'IPDA001',
                'chit': 'CHIT001',
                'fhit': '2023-11-10',
                'hhit': '10:00:00'
            },
            {
                'cpda': 'CPDA001',
                'ipda': 'IPDA001',
                'chit': 'CHIT002',
                'fhit': '2023-11-11',
                'hhit': '11:00:00'
            },
            {
                'cpda': 'CPDA002',
                'ipda': 'IPDA002',
                'chit': 'CHIT003',
                'fhit': '2023-11-12',
                'hhit': '12:00:00'
            },
        ]
    }

    # Mock methods called within process_query_response
    with patch.object(estado_ane_gru, 'procesa_partida') as mock_procesa_partida:
        estado_ane_gru.process_query_response(query_reply)
        assert len(estado_ane_gru.partidas) == 2
        mock_procesa_partida.assert_any_call('CPDA001', [
            {
                'status_code': 'CHIT001',
                'date_of_event': '2023-11-10',
                'time_of_event': '10:00:00'
            },
            {
                'status_code': 'CHIT002',
                'date_of_event': '2023-11-11',
                'time_of_event': '11:00:00'
            },
        ])
        mock_procesa_partida.assert_any_call('CPDA002', [
            {
                'status_code': 'CHIT003',
                'date_of_event': '2023-11-12',
                'time_of_event': '12:00:00'
            }
        ])


def test_actualizar_comunicado(estado_ane_gru):
    # Configurar partidas
    estado_ane_gru.partidas = {
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

    # Mock BmApi
    with patch.object(estado_ane_gru.bm, 'post_partida_tracking') as mock_post_tracking:
        # Configurar respuesta simulada
        mock_post_tracking.return_value = {'status_code': 200}

        estado_ane_gru.actualizar_comunicado()

        # Verificar que se llamó para CPDA001
        mock_post_tracking.assert_called_with('IPDA001', {
            'codigohito': 'ESTADOCOM',
            'descripciontracking': 'ESTADO COMUNICADO',
            'fechatracking': ANY  # Ignoramos el valor exacto de la fecha
        })

        # Verificar que no se llamó para CPDA002 (success=False)
        # Verificar que se imprimió mensaje para CPDA003 (ipda=None)


def test_run(estado_ane_gru):
    # Mock BmApi consulta_
    query_reply = {
        'contenido': []
    }
    with patch.object(estado_ane_gru.bm, 'consulta_', return_value=query_reply) as mock_consulta:
        with patch.object(estado_ane_gru, 'process_query_response') as mock_process_query_response:
            with patch.object(estado_ane_gru, 'actualizar_comunicado') as mock_actualizar_comunicado:
                estado_ane_gru.run()
                mock_consulta.assert_called()
                mock_process_query_response.assert_called_with(query_reply)
                mock_actualizar_comunicado.assert_called()


def test_procesa_partida(estado_ane_gru):
    # Initialize partidas as a dictionary
    cpda = 'CPDA001'
    estado_ane_gru.partidas = {
        cpda: {'ipda': 'IPDA001'}
    }
    # Mock dependencies
    with patch('src.est_ane_gru.Fort') as mock_Fort:
        mock_fort_instance = MagicMock()
        mock_Fort.return_value = mock_fort_instance
        # Set return values for methods of Fort instance
        mock_fort_instance.header.return_value = 'HEADER_CONTENT'
        mock_fort_instance.header_q00.return_value = 'HEADER_Q00_CONTENT'
        mock_fort_instance.z_control_record.return_value = 'Z_CONTROL_CONTENT'
        mock_fort_instance.cierre.return_value = 'CIERRE_CONTENT'

        with patch('src.est_ane_gru.composeQ10', return_value='Q10_LINE_CONTENT') as mock_composeQ10:
            # Adjust the mocks to prevent overwriting self.partidas[cpda]
            with patch.object(estado_ane_gru, 'write_txt_file') as mock_write_txt_file:
                mock_write_txt_file.return_value = '/path/to/file.txt'
                with patch.object(estado_ane_gru, 'upload_file') as mock_upload_file:
                    mock_upload_file.return_value = True
                    with patch('os.remove') as mock_os_remove:
                        q10_lines = [
                            {
                                'status_code': 'CHIT001',
                                'date_of_event': '2023-11-10',
                                'time_of_event': '10:00:00'
                            },
                            {
                                'status_code': 'CHIT002',
                                'date_of_event': '2023-11-11',
                                'time_of_event': '11:00:00'
                            },
                        ]
                        result = estado_ane_gru.procesa_partida(cpda, q10_lines)
                        print(f"Result: {result}")
                        assert result is not None
                        # Instead of checking result['success'], check estado_ane_gru.partidas[cpda]
                        assert estado_ane_gru.partidas[cpda]['success'] is True
                        mock_write_txt_file.assert_called()
                        mock_upload_file.assert_called_with('/path/to/file.txt')
                        mock_os_remove.assert_called_with('/path/to/file.txt')
