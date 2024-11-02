import os
import json
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'conf', '.env.base')
load_dotenv(dotenv_path)

class GetToken:
    def __init__(self, entorno="dev"):
        self.entorno = entorno
        self.token_file = os.path.join(os.path.dirname(__file__), '..', 'fixtures', 'token.json')

        # Cargar las variables de entorno desde el archivo .env
        self.bm_api_url = os.getenv('BM_API_AUTH_URL')
        self.user = os.getenv('USER_ANE_TEST') if entorno == "dev" else os.getenv('USER_ANE')
        self.password = os.getenv('PW_ANE_TEST') if entorno == "dev" else os.getenv('PW_ANE')
        self.headers = {'Content-Type': 'application/json', 'encoding': 'charset=utf-8'}

    def verificar_token(self):
        # Si el archivo token.json no existe, lo creamos
        if not os.path.exists(self.token_file):
            print(f"Archivo '{self.token_file}' no existe, creando uno nuevo...")
            self.crear_archivo_token()

        # Leer el archivo JSON
        with open(self.token_file, 'r') as f:
            data = json.load(f)

        # Determinar si es entorno de "dev" o producción
        if self.entorno == "dev":
            token_key, fecha_key = "token_dev", "fecha_dev"
        else:
            token_key, fecha_key = "token_prod", "fecha_prod"

        # Si el valor del token es vacío o la fecha está desactualizada
        if not data[token_key] or self.diferencia_horas(data[fecha_key]) >= 12:
            print(f"Token desactualizado o vacío, solicitando un nuevo token...")
            nuevo_token = self.solicitar_nuevo_token()
            if nuevo_token:
                # Actualizamos el archivo con el nuevo token y la fecha actual
                data[token_key] = nuevo_token
                data[fecha_key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with open(self.token_file, 'w') as f:
                    json.dump(data, f, indent=4)
                return f"Bearer {nuevo_token}"
        else:
            return f"Bearer {data[token_key]}"

    @staticmethod
    def diferencia_horas(fecha_str: str) -> float:
        """Calcula la diferencia en horas entre la fecha del token y la actual"""
        fecha_token = datetime.strptime(fecha_str, "%Y-%m-%d %H:%M:%S")
        ahora = datetime.now()
        diferencia = ahora - fecha_token
        return diferencia.total_seconds() / 3600  # Retorna la diferencia en horas

    def solicitar_nuevo_token(self):
        """Solicita un nuevo token a la API y maneja los reintentos en caso de error"""
        print(f"Solicitando nuevo token {self.entorno}...")
        intento = 0
        while intento < 3:
            try:
                respuesta = requests.post(
                    self.bm_api_url,
                    json={"usuario": self.user, "clave": self.password},
                    headers=self.headers
                )
                if respuesta.status_code == 200:
                    # Asumimos que el token viene en el body de la respuesta
                    token = respuesta.json().get('token')
                    if token:
                        return token
                    else:
                        raise ValueError("No se encontró el token en la respuesta")
                else:
                    print(f"Error {respuesta.status_code}: {respuesta.text}")
            except requests.RequestException as e:
                print(f"Error en la solicitud: {e}")

            # Esperamos 10 segundos antes de reintentar
            intento += 1
            print(f"Esperando 15 segundos antes de reintentar... ({intento}/3)")
            time.sleep(10)

        print("Se han realizado 10 intentos fallidos, interrumpiendo el programa.")
        return None

    def crear_archivo_token(self) -> None:
        """Crea el archivo token.json con valores vacíos"""
        data = {
            "token_prod": "",
            "fecha_prod": "",
            "token_dev": "",
            "fecha_dev": ""
        }
        os.makedirs(os.path.dirname(self.token_file), exist_ok=True)
        with open(self.token_file, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Archivo '{self.token_file}' creado con éxito.")


if __name__ == "__main__":
    def main():
        entorno = "dev"
        get_token_instance = GetToken()
        token = get_token_instance.verificar_token()
        print(f"Token obtenido: {token}")