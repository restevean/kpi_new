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

def n_ref(trip_ref=''):
  # 2024MO12103  --> 12103/2024/MO
  trip_=trip_ref.strip() # 2024MO12103
  return f"{trip_[6:]}/{trip_[:4]}/{trip_[4:6]}"


