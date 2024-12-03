import boto3
import json


def safe_get_token(context: str = None) -> str:
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    # lambda_client = boto3.client('lambda')

    payload = {"token_type": context} if context else {}

    try:
        # Invocar la función Lambda
        response = lambda_client.invoke(
            FunctionName="RenewTokensFunction",  # Asegúrate de que este nombre es correcto
            InvocationType="RequestResponse",  # Sincrónica
            Payload=json.dumps(payload)
        )

        # Leer el cuerpo de la respuesta
        response_payload = json.loads(response['Payload'].read().decode("utf-8"))
        if token := response_payload.get('token'):
            print(f"Token obtenido: {token}")
            return token
        else:
            return None

    except Exception as e:
        print(f"Error invocando la función Lambda: {e}")
        raise


if __name__ == "__main__":
    safe_get_token()
    safe_get_token(context="dev")
    safe_get_token(context="prod")
