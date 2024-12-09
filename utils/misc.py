# utils/misc.py

from datetime import datetime, time
from typing import Optional


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


def n_ref(trip_ref='', mode=""): # "r" de reverse
    # Mode == "r" 12103/2024/MO  --> 2024MO12103
    # With no mode 2024MO12103  --> 12103/2024/MO
    if trip_ref:
        trip_=trip_ref.strip() # 2024MO12103
        if mode == "r":
            parts = trip_ref.split("/")
            return f"{parts[1]}{parts[2]}{parts[0]}"
        else:
            return f"{trip_[6:]}/{trip_[:4]}/{trip_[4:6]}"
    return " " * 10

