# tests/test_safe_get_token.py

import pytest
import json  # Importar json para las pruebas
from unittest.mock import patch, MagicMock
from utils.safe_get_token import safe_get_token


@patch("utils.safe_get_token.boto3.Session")
def test_safe_get_token_success(mock_session):
    # Mock de la sesión de boto3
    mock_lambda_client = MagicMock()
    mock_session.return_value.client.return_value = mock_lambda_client

    # Respuesta simulada de Lambda
    mock_response_payload = {
        "token": "mocked_token",
        "status": "success"
    }
    mock_lambda_client.invoke.return_value = {
        "Payload": MagicMock(read=lambda: json.dumps(mock_response_payload).encode("utf-8"))
    }

    # Llamada a la función y verificación del resultado
    result = safe_get_token()
    assert result == "mocked_token"
    mock_lambda_client.invoke.assert_called_once_with(
        FunctionName="RenewTokensFunction",
        InvocationType="RequestResponse",
        Payload="{}"
    )


@patch("utils.safe_get_token.boto3.Session")
def test_safe_get_token_with_context(mock_session):
    # Mock de la sesión de boto3
    mock_lambda_client = MagicMock()
    mock_session.return_value.client.return_value = mock_lambda_client

    # Respuesta simulada de Lambda
    mock_response_payload = {
        "token": "mocked_context_token",
        "status": "success"
    }
    mock_lambda_client.invoke.return_value = {
        "Payload": MagicMock(read=lambda: json.dumps(mock_response_payload).encode("utf-8"))
    }

    # Llamada a la función con un contexto específico
    result = safe_get_token(context="dev")
    assert result == "mocked_context_token"
    mock_lambda_client.invoke.assert_called_once_with(
        FunctionName="RenewTokensFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({"token_type": "dev"})
    )


@patch("utils.safe_get_token.boto3.Session")
def test_safe_get_token_no_token(mock_session):
    # Mock de la sesión de boto3
    mock_lambda_client = MagicMock()
    mock_session.return_value.client.return_value = mock_lambda_client

    # Respuesta simulada sin token
    mock_response_payload = {
        "status": "success"
    }
    mock_lambda_client.invoke.return_value = {
        "Payload": MagicMock(read=lambda: json.dumps(mock_response_payload).encode("utf-8"))
    }

    # Llamada a la función y verificación del resultado
    result = safe_get_token()
    assert result is None
    mock_lambda_client.invoke.assert_called_once_with(
        FunctionName="RenewTokensFunction",
        InvocationType="RequestResponse",
        Payload="{}"
    )


@patch("utils.safe_get_token.boto3.Session")
def test_safe_get_token_exception(mock_session):
    # Mock de la sesión de boto3 que lanza una excepción
    mock_session.return_value.client.side_effect = Exception("Mocked exception")

    # Llamada a la función y verificación de que lanza una excepción
    with pytest.raises(Exception, match="Mocked exception"):
        safe_get_token()
