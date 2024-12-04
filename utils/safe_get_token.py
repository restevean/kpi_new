import boto3
import json


def safe_get_token(context: str = None) -> str:
    try:
        # Configura la sesión con el perfil específico
        session = boto3.Session(profile_name='Anexa_IAM')
        lambda_client = session.client('lambda', region_name='us-east-1')

        # Construye el payload
        payload = {"token_type": context} if context else {}

        # Invoca la función Lambda
        response = lambda_client.invoke(
            FunctionName="RenewTokensFunction",  # Asegúrate de que el nombre es correcto
            InvocationType="RequestResponse",   # Invocación sincrónica
            Payload=json.dumps(payload)
        )

        # Procesa la respuesta
        response_payload = json.loads(response['Payload'].read().decode("utf-8"))
        if token := response_payload.get('token'):
            print(f"{response_payload.get('status')}\nToken obtenido: \n{token}")
            return token
        else:
            print("No se obtuvo ningún token en la respuesta.")
            return None

    except Exception as e:
        print(f"Error invocando la función Lambda: {e}")
        raise


if __name__ == "__main__":
    # Prueba con diferentes contextos
    safe_get_token()
    safe_get_token(context="dev")
    safe_get_token(context="pro")
