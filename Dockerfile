# Usa Python 3.12 como base
FROM python:3.12-slim

# Instala Poetry
RUN pip install --upgrade pip && pip install poetry

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /scripts

# Copia los archivos de configuración de Poetry
COPY pyproject.toml poetry.lock /scripts/

# Instala las dependencias usando Poetry
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# Copia los scripts al contenedor
COPY src/ /scripts/src/

# Comando para mantener el contenedor en ejecución
CMD ["tail", "-f", "/dev/null"]
