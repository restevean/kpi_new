import pytest
from utils.misc import fecha_iso, date_yyyy_mm_dd, n_ref


def test_fecha_iso():
    # Caso básico
    assert fecha_iso("20231209") == "2023-12-09T01:00.000Z"
    # Formato personalizado
    assert fecha_iso("09-12-2023", "%d-%m-%Y") == "2023-12-09T01:00.000Z"
    # Fecha inválida
    with pytest.raises(ValueError):
        fecha_iso("20231235")  # Fecha no válida


def test_date_yyyy_mm_dd():
    # Caso básico sin hora
    assert date_yyyy_mm_dd("20231209") == "2023-12-09T11:00:00"
    # Caso con hora específica
    assert date_yyyy_mm_dd("20231209", "0930") == "2023-12-09T09:30:00"
    # Hora no válida
    with pytest.raises(ValueError):
        date_yyyy_mm_dd("20231209", "2560")  # Hora no válida
    # Fecha no válida
    with pytest.raises(ValueError):
        date_yyyy_mm_dd("20231235")  # Fecha no válida


def test_n_ref_reverse_mode():
    # Caso básico en modo reverse
    assert n_ref("12103/2024/MO", mode="r") == "2024MO12103"
    # Referencia vacía
    assert n_ref("", mode="r") == "          "
    # Formato incorrecto
    with pytest.raises(IndexError):
        n_ref("12103", mode="r")  # Falta el formato adecuado


def test_n_ref_default_mode():
    # Caso básico sin modo
    assert n_ref("2024MO12103") == "12103/2024/MO"
    # Referencia vacía
    assert n_ref("") == "          "
    # Formato incorrecto (la función genera un resultado con base en la lógica actual)
    # El resultado esperado debe reflejar lo que produce la función actualmente
    assert n_ref("MO12103") == "3/MO12/10"


def test_n_ref_invalid_input():
    # Referencia vacía
    assert n_ref("") == "          "
    # Referencia con caracteres especiales (resultado según la lógica actual)
    assert n_ref("!@#$%^&*()") == "&*()/!@#$/%^"
    # Referencia demasiado corta (resultado según la lógica actual)
    assert n_ref("MO12") == "/MO12/"
    # Referencia sin suficiente estructura para modo "r" (debe lanzar IndexError)
    with pytest.raises(IndexError):
        n_ref("MO12103", mode="r")
    # Referencia sin un formato esperado para modo "r" (debe lanzar IndexError)
    with pytest.raises(IndexError):
        n_ref("12345MO", mode="r")
    # Formato válido pero con longitud incorrecta en modo "r" (debe lanzar IndexError)
    with pytest.raises(IndexError):
        n_ref("MO12103/123", mode="r")
