import os
from dotenv import load_dotenv
from utils import fortras_stat as state
from utils.bmaster_api import BmasterApi as BmApi
from datetime import datetime, timezone
from utils.send_email import EmailSender


load_dotenv(dotenv_path='../conf/.env.base')
ENTORNO = os.getenv("ENTORNO")



def estado_grub_anex():
    work_directory = "../fixtures"
    email_from = "Gruber Estado"
    email_subject = "Gruber Estado"
    email_body = None
    if ENTORNO == "prod":
        email_to = ["restevean@gmail.com"]
    else:
        email_to = ["restevean@gmail.com", "resteve24@gmail.com"]

    files = load_dir_files(work_directory)
    # Procesamos cada archivo
    for file in files:
        if file_process(work_directory + "/" + file):
            files[file]["success"] = True

    # Movemos los archivos a la carpeta correspondiente en función de success
    # Enviamos los correos si success = True
    new_email = EmailSender()
    new_email.send_email(email_from, email_to, email_subject, "")


def load_dir_files(directory):

    files_in_dir = {
        file: {
            "success": False,
            "file_name": None,
            "message": None,
            "misc": None
        }
        for file in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, file))
    }
    print(files_in_dir)
    return files_in_dir

    # Enviamos los correos
    # Movemos los archivos a las carpetas correspondientes


def get_cod_hito(status):
    status_map = {
        "001": "ENT001",
        "002": "DESCARFAL",
        "003": "DESCARKO",
        "SPC": "SAL001",
        "COR": "ANXE05"
    }
    return status_map.get(status)


def file_process(file_name):
    # data = state.MensajeEstado()
    # results = data.leer_stat_gruber(file_name)
    results = state.MensajeEstado().leer_stat_gruber(file_name)
    for lineas in results.get("Lineas", []):
        if lineas.get("Record type ‘Q10’") == "Q10":
            n_ref_cor = lineas.get("Consignment number sending depot")
            n_status = lineas.get("Status code")
            m_query = f"select ipda, * from trapda where nrefcor ='{n_ref_cor}'"
            print(m_query) # Eliminar en producción
            bm = BmApi()
            query_reply = bm.consulta_(m_query)
            print(query_reply["status_code"]) # Eliminar en producción
            print(query_reply) # Eliminar en producción
            if "contenido" in query_reply and query_reply["contenido"]:
                n_ipda = query_reply["contenido"][0].get("ipda")
                n_json = {
                    "codigohito": get_cod_hito(n_status),
                    "descripciontracking": file_name,
                    "fechatracking": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                }
                # tracking_reply = bm.post_partida_tracking(n_ipda, query_reply["contenido"][0])
                tracking_reply = bm.post_partida_tracking(n_ipda, n_json)
                if  tracking_reply["status_code"] == 201:
                    print("Success")

                    # Enviar email y mover fichero a exito
                else:
                    print("Fail")
            else:
                n_ipda = None  # O asigna otro valor predeterminado si lo prefieres
            print(f"El ipda es: {n_ipda}")

    # Devuelve True si hemos tenido éxito y False si no


if __name__ == "__main__":
    # estado_grub_anex()
    file_process("../fixtures/STAT512.example_ANEXA.cd2babfd-5f19-40c1-afed-68693095597e")

"""
    fichero = "../fixtures/STAT512.example_ANEXA.cd2babfd-5f19-40c1-afed-68693095597e"
    try:
        file_process(fichero)
    except FileNotFoundError:
        print("Archivo no encontrado. Verifica la ruta y el nombre.")
"""


"""
Glosario:
ipda        código interno (para el ERP BM) de bm de la partida
cpda        código externo (para el ERP BM) de bm de la partida
nrefcor     referencia del corresponsal, no tiene ancho predeterminado, va desde el carácter 4 de la línea que empieza 
            por "Q10" hasta el espacio en blanco (excluido)

Recibimos el archivo por sftp: 
¿de qué sftp?
¿usuario sftp?
¿contraseña sftp?
¿ruta de descarga?
¿nombre del archivo?

La consulta (paso 6) me devuelve 200, [], seguramente porque la bbdd no tiene datos d¡que devolver.


x 1. leer usando fortras_stat - Método Gruber
x 2. lee lineas y recibes el diccionario de cada linea
x 3. formato nrefcor 12345-2024-VR que es su referencia
x 4. query = "select ipda, * from trapda where nrefcor = '12345-2024-VR'"
x 5. resp_consulta='bm.consutla_(query)'
x 6. ipda=23454. 

json _tracking {}
ipda 
código de hito
Q10 fortras, stauscode -> código hito en la excel


resp_tracking = post_partida_trakcingf(id=ipda, json_tracking)

if resp_tracking["cod_error"]==201:
    'exito'
    enviar email
    mover fichero a exito
else:
    'houston, we have a problem'
    email
    'mover fifchero a fracaso'
    
Se procesan todos los Q10 de un fichero
"""

