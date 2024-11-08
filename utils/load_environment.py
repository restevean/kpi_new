import os
from dotenv import load_dotenv

def load_environment():
    if os.getenv("ENTORNO") is None:
        # Base environment
        load_dotenv(dotenv_path="../conf/.env.base")
        os.getenv("ENTORNO")
        INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
        EMAIL_OURS = os.getenv("EMAIL_OURS")

        # Customer environment
        load_dotenv(dotenv_path=f"../conf/.env.'+{INTEGRATION_CUST}")
        os.getenv("EMAIL_TO")
        SFTP_SERVER = os.getenv("SFTP_SERVER")
        SFTP_USER = os.getenv("SFTP_USER")
        SFTP_PW = os.getenv("SFTP_PW")
        SFTP_PORT = os.getenv("SFTP_PORT")
        USER_CUST_TEST = "sw.anexa.curso"
        PW_CUST_TEST = "sPh42bQAvc"
        USER_CUST = "sw.anexa.produccion"
        PW_CUST = "UmSCD5BspE"
        SMTP_SERVER = "smtp.gmail.com"
        SMTP_PORT = "465"
        SMTP_USERNAME = "integraciones@anexalogistica.com"
        SMTP_PW = "i5486ntegraciones"

if __name__ == "__main__":
    load_environment()
    pause = input("Press Enter to continue...")
    print("Environment variables loaded successfully.")