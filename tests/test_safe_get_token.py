import json
import pytest
from unittest.mock import patch, MagicMock
from utils.safe_get_token import safe_get_token

@pytest.fixture
def mock_lambda_client():
    with patch("boto3.client") as mock_client:
        yield mock_client


def test_safe_get_token_no_context(mock_lambda_client):
    # Configurar respuesta simulada de Lambda
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({"token": "mock_token"}).encode("utf-8")
    mock_lambda_client.return_value.invoke.return_value = {"Payload": mock_response}

    # Llamar a la función sin pasar contexto
    token = safe_get_token()

    # Verificar que el token sea correcto
    assert token == "mock_token"
    mock_lambda_client.return_value.invoke.assert_called_once_with(
        FunctionName="RenewTokensFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({})
    )


def test_safe_get_token_with_test_context(mock_lambda_client):
    # Configurar respuesta simulada de Lambda
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({"token": "mock_test_token"}).encode("utf-8")
    mock_lambda_client.return_value.invoke.return_value = {"Payload": mock_response}

    # Llamar a la función pasando contexto "test"
    token = safe_get_token(context="test")

    # Verificar que el token sea correcto
    assert token == "mock_test_token"
    mock_lambda_client.return_value.invoke.assert_called_once_with(
        FunctionName="RenewTokensFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({"token_type": "test"})
    )


def test_safe_get_token_with_prod_context(mock_lambda_client):
    # Configurar respuesta simulada de Lambda
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({"token": "mock_prod_token"}).encode("utf-8")
    mock_lambda_client.return_value.invoke.return_value = {"Payload": mock_response}

    # Llamar a la función pasando contexto "prod"
    token = safe_get_token(context="prod")

    # Verificar que el token sea correcto
    assert token == "mock_prod_token"
    mock_lambda_client.return_value.invoke.assert_called_once_with(
        FunctionName="RenewTokensFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({"token_type": "prod"})
    )


def test_safe_get_token_missing_token(mock_lambda_client):
    # Configurar respuesta simulada de Lambda sin 'token' en el body
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({}).encode("utf-8")
    mock_lambda_client.return_value.invoke.return_value = {"Payload": mock_response}

    # Verificar que se lance un KeyError cuando no haya 'token' en la respuesta
    with pytest.raises(KeyError):
        safe_get_token(context="test")
