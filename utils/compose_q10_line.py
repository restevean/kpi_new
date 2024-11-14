# TODO Include this function as a method in fortras
def compose_q10_line(**kwargs):
    # Definimos los campos con sus valores por defecto (rellenos de espacios) y anchos específicos
    fields = {
        "record_type": ("Q10", 3),
        "consignment_number_sending_depot": ("", 35),
        "consignment_number_receiving_depot": ("", 35),
        "Pick-up_order_number": ("", 35),
        "status_code": ("", 3),
        "date_of_event": ("", 8),  # Formato "YYYYMMDD"
        "time_of_event": ("", 4),  # Formato "HHMM"
        "consignment_number_delivering_party": ("", 35),
        "wait_downtime_in_minutes": ("", 4),
        "name_of_acknowledging_party": ("", 35),
        "additional_text": ("", 70),
        "reference_number": (" " * 12, 12),  # 12 caracteres de espacio en blanco
        "latitude": ("", 11),
        "longitude": ("", 11),
    }

    # Construimos la cadena resultante
    result = ""

    for field, (default, width) in fields.items():
        # Obtener el valor del argumento o usar el valor por defecto
        value = kwargs.get(field, default)

        # Convertir `date_of_event` al formato "YYYYMMDD" si es cadena en formato esperado
        if field == "date_of_event" and isinstance(value, str) and len(value) >= 10:
            value = value[:10].replace("-", "")  # Convierte "YYYY-MM-DD" en "YYYYMMDD"

        # Convertir `time_of_event` al formato "HHMM" si es cadena en formato esperado
        elif field == "time_of_event" and isinstance(value, str) and len(value) >= 5:
            value = value.replace(":", "")[:4]  # Convierte "HH:MM" en "HHMM"

        # Asegurar que el valor es una cadena y ajustarlo al ancho especificado
        value_str = str(value).ljust(width)[:width]
        result += value_str

    return result + "\n"  # Añadir un salto de línea al final


if __name__ == "__main__":
    # Ejemplo de uso
    line = compose_q10_line(
        status_code="A",
        date_of_event="2024-10-01T00:00:00",
        time_of_event="11:38",
    )
    print(repr(line))  # Muestra la cadena con un salto de línea al final


