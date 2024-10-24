import os
import json
import pytest
from unittest.mock import patch, mock_open
from requests import Response
from utils.get_token import GetToken

# Fixture para entorno "curso"
@pytest.fixture
def entorno_curso():
    return "curso"

# Fixture para simular la ruta del archivo token.json
@pytest.fixture
def token_file_path():
    return os.path.join(os.path.dirname(__file__), '..', 'fixtures', 'token.json')


# Test para verificar la creación del archivo token.json si no existe
@patch("os.path.join", return_value='/Users/rafaelesteve/PycharmProjects/kpi_new/tests/../fixtures/token.json')
def test_crear_archivo_token(mock_path_join, tmpdir, entorno_curso):
    """Test para verificar la creación del archivo token.json si no existe."""
    # Mock de open para crear el archivo
    with patch("builtins.open", mock_open()) as mock_file:
        get_token = GetToken(entorno_curso)
        get_token.crear_archivo_token()

        # Verificar que se abrió el archivo en modo 'w'
        mock_file.assert_called_once_with('/Users/rafaelesteve/PycharmProjects/kpi_new/tests/../fixtures/token.json', 'w')

        # Concatenar todas las llamadas a write() y verificar el contenido escrito
        handle = mock_file()
        write_calls = ''.join(call[0][0] for call in handle.write.call_args_list)
        expected_content = json.dumps({
            "token_prod": "",
            "fecha_prod": "",
            "token_curso": "",
            "fecha_curso": ""
        }, indent=4)
        assert write_calls == expected_content


# Test para verificar que se solicita un nuevo token correctamente
@patch("utils.get_token.requests.post")
def test_solicitar_nuevo_token(mock_post, entorno_curso):
    """Test para verificar que se solicita un nuevo token correctamente."""
    # Simular una respuesta HTTP válida
    mock_response = Response()
    mock_response.status_code = 200
    mock_response._content = b'{"token": "nuevo_token_curso"}'  # Simular la respuesta JSON con un token
    mock_post.return_value = mock_response

    get_token = GetToken(entorno_curso)
    nuevo_token = get_token.solicitar_nuevo_token()

    # Verificar que se hizo la solicitud POST
    mock_post.assert_called_once_with(
        get_token.bm_api_url,
        json={"usuario": get_token.user, "clave": get_token.password},
        headers=get_token.headers
    )

    # Verificar que se devolvió el token correcto
    assert nuevo_token == "nuevo_token_curso"


# Test para verificar el comportamiento cuando falla la solicitud de token
@patch("utils.get_token.requests.post")
def test_solicitar_nuevo_token_falla(mock_post, entorno_curso):
    """Test para verificar el comportamiento cuando falla la solicitud de token."""
    # Simular una respuesta HTTP inválida (500 Internal Server Error)
    mock_response = Response()
    mock_response.status_code = 500
    mock_response._content = b'{"error": "server_error"}'
    mock_post.return_value = mock_response

    get_token = GetToken(entorno_curso)
    nuevo_token = get_token.solicitar_nuevo_token()

    # Verificar que se hizo la solicitud POST 3 veces debido a los reintentos
    assert mock_post.call_count == 3

    # Verificar que el nuevo token es None cuando falla
    assert nuevo_token is None


# Test para verificar si un token existente es válido y no se renueva
@patch("builtins.open", new_callable=mock_open, read_data='{"token_curso": "token_viejo", "fecha_curso": "2024-10-23 00:16:49"}')
@patch("os.path.exists", return_value=True)
def test_verificar_token_valido(mock_exists, mock_open_file, entorno_curso):
    """Test para verificar si un token existente es válido y no se renueva."""
    get_token = GetToken(entorno_curso)

    # Simular que el token ya es válido (diferencia de tiempo menor a 12 horas)
    with patch.object(GetToken, 'diferencia_horas', return_value=6):
        token = get_token.verificar_token()

        # Asegurarse de que no se llamó a solicitar_nuevo_token
        assert token == "Bearer token_viejo"
        mock_open_file.assert_called_once_with(get_token.token_file, 'r')


# Test para verificar que se renueva el token si ha expirado
@patch("builtins.open", new_callable=mock_open, read_data='{"token_curso": "token_viejo", "fecha_curso": "2024-10-22 00:16:49"}')
@patch("os.path.exists", return_value=True)
@patch.object(GetToken, 'solicitar_nuevo_token', return_value="nuevo_token_curso")
def test_verificar_token_expirado(mock_solicitar, mock_exists, mock_open_file, entorno_curso):
    """Test para verificar que se renueva el token si ha expirado."""
    get_token = GetToken(entorno_curso)

    # Simular que el token ha expirado (diferencia de tiempo mayor a 12 horas)
    with patch.object(GetToken, 'diferencia_horas', return_value=14):
        token = get_token.verificar_token()

        # Asegurarse de que se solicitó un nuevo token
        mock_solicitar.assert_called_once()
        assert token == "Bearer nuevo_token_curso"
