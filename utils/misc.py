# utils/misc.py

from datetime import datetime, time
from typing import Optional
from charset_normalizer import from_path
from pathlib import Path


# Empezamos con pathlib y obtenemos el path
utils_dir = Path(__file__).resolve().parent
file_dir = utils_dir.parent / "fixtures" / "xbs" / "edi"

"""
Funciones auxiliares
"""


def fecha_iso(fecha_entrada: str = "", formato: str = "%Y%m%d") -> str:
    fecha = datetime.strptime(fecha_entrada, formato)
    return fecha.strftime("%Y-%m-%d") + "T01:00.000Z"


def date_yyyy_mm_dd(fecha: str = "", hora: Optional[str] = None) -> str:
    fecha_dt = datetime.strptime(fecha, "%Y%m%d")
    if hora:
        hora_dt = datetime.strptime(hora, "%H%M").time()
    else:
        hora_dt = time(11, 0)  # 11:00 AM como valor predeterminado
    fecha_completa = datetime.combine(fecha_dt.date(), hora_dt)
    return fecha_completa.strftime("%Y-%m-%dT%H:%M:%S")


# def n_ref(trip_ref='', mode=""): # "r" de reverse
#     # Mode == "r" 12103/2024/MO  --> 2024MO12103
#     # With no mode 2024MO12103  --> 12103/2024/MO
#     if trip_ref:
#         trip_=trip_ref.strip() # 2024MO12103
#         if mode == "r":
#             parts = trip_ref.split("/")
#             return f"{parts[1]}{parts[2]}{parts[0]}"
#         else:
#             return f"{trip_[6:]}/{trip_[:4]}/{trip_[4:6]}"
#     return " " * 10


def n_ref(trip_ref='', mode=""):  # "r" de reverse
    """
    - Modo "r":
        12103/2024/MO  --> 2024MO12103
        Debe comprobar que 'trip_ref' contenga exactamente 2 barras '/'
        y que no estén consecutivas ("//"). Si el formato no es correcto,
        devuelve '' (cadena vacía).
    - Modo por defecto (sin "r"):
        2024MO12103 --> 12103/2024/MO
    """

    if trip_ref:
        trip_ = trip_ref.strip()
        if mode == "r":
            # Validar que existan exactamente 2 "/" y que no haya "//"
            if trip_.count("/") == 2 and "//" not in trip_:
                parts = trip_.split("/")
                return f"{parts[1]}{parts[2]}{parts[0]}"
            else:
                return " " * 10 # No cumple formato
        else:
            # Sin modo "r", asumimos que el formato es 2024MO12103
            return f"{trip_[6:]}/{trip_[:4]}/{trip_[4:6]}"

    return " " * 10



def comprobar_codificacion(filepath):
    resultado = from_path(filepath).best()
    return resultado.encoding, resultado


def convertir_a_utf8(ruta_entrada, ruta_salida):
    # Detectar la codificación del archivo original
    resultado = from_path(ruta_entrada).best()
    if not resultado:
        raise ValueError("No se pudo detectar la codificación del archivo.")

    texto = str(resultado)  # Convertir el contenido a texto
    encoding_original = resultado.encoding

    # Guardar el archivo como UTF-8
    with open(ruta_salida, 'w', encoding='utf-8') as archivo_salida:
        archivo_salida.write(texto)

    print(f"Archivo convertido de {encoding_original} a UTF-8 y guardado en {ruta_salida}.")


# Ruta del archivo original y archivo convertido



if __name__ == "__main__":

    # Comprobar la codificación
    codificacion, detalle = comprobar_codificacion(file_dir / "BORD512_1ES24ET1865_31148_16122024-152903.txt")
    if codificacion == 'utf_8':
        print(f"El archivo {file_dir / "BORD512_1ES24ET1865_31148_16122024-152903.txt"} está codificado en UTF-8.")
    else:
        print(f"El archivo {file_dir / "BORD512_1ES24ET1865_31148_16122024-152903.txt"} no está en UTF-8, está en {codificacion}.")


    # Convertirlo a UTF-8
    ruta_entrada = file_dir / "BORD512_1ES24ET1865_31148_16122024-152903.txt"
    ruta_salida = file_dir / "bor_xbs.txt"

    try:
        convertir_a_utf8(ruta_entrada, ruta_salida)
    except Exception as e:
        print(f"Error: {e}")