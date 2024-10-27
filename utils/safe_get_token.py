import boto3
import json


def safe_get_token(context: str = None) -> str:

    # Crear un cliente para Lambda
    lambda_client = boto3.client('lambda')

    # Construir el payload para la función Lambda
    payload = {}
    if context:
        payload["token_type"] = context  # Cambiado a "token_type" para que coincida con la Lambda

    try:
        # Invocar la función Lambda
        response = lambda_client.invoke(
            FunctionName="RenewTokensFunction",
            InvocationType="RequestResponse",  # Sincrónica
            Payload=json.dumps(payload)
        )

        # Leer el cuerpo de la respuesta
        response_payload = json.loads(response['Payload'].read().decode("utf-8"))

        # Extraer el token de la respuesta
        print(response_payload['token'])
        return response_payload['token']

    except Exception as e:
        print(f"Error invoking Lambda function: {e}")
        raise


if __name__ == "__main__":
    safe_get_token()
    safe_get_token(context="test")
    safe_get_token(context="prod")
