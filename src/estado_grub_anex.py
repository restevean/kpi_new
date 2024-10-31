import os
from dotenv import load_dotenv
from utils import fortras_stat as state
from bmaster_api import BmasterApi as BmApi


load_dotenv(dotenv_path='../conf/.env')
ENTORNO = os.getenv("ENTORNO")


def estado_grub_anex(file_name):
    data = state.MensajeEstado()
    results = data.leer_stat_gruber(file_name)

    for lineas in results.get("Lineas", []):
        if lineas.get("Record type ‘Q10’") == "Q10":
            n_ref_cor = lineas.get("Consignment number sending depot", "").strip()
            m_query = f"select ipda, * from trapda where nrefcor ='{n_ref_cor}'"
            print(m_query) # Eliminar en producción
            bm = BmApi()
            query_reply = bm.consulta_(m_query)
            print(query_reply["status_code"]) # Eliminar en producción
            print(query_reply) # Eliminar en producción
            if "contenido" in query_reply and query_reply["contenido"]:
                n_ipda = query_reply["contenido"][0].get("ipda")
            else:
                n_ipda = None  # O asigna otro valor predeterminado si lo prefieres
            print(f"El ipda es: {n_ipda}")



if __name__ == "__main__":
    fichero = "../fixtures/STAT512.example_ANEXA.cd2babfd-5f19-40c1-afed-68693095597e"
    try:
        estado_grub_anex(fichero)
    except FileNotFoundError:
        print("Archivo no encontrado. Verifica la ruta y el nombre.")


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

