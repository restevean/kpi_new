from utils.get_token import GetToken

# Usar la clase GetToken para obtener el token
def main():
    entorno = "curso"
    # et_token_instance = GetToken(entorno)
    get_token_instance = GetToken()
    token = get_token_instance.verificar_token()
    print(f"Token obtenido: {token}")

if __name__ == "__main__":
    main()