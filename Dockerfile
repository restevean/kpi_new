# kpi_new/Dockerfile

# Usa Python 3.12 como imagen base
FROM python:3.12-slim

# Crear el directorio /home/root/.aws
RUN mkdir -p /root/.aws

# Copiar el archivo credentials desde .aws/credentials en la raíz del proyecto
COPY .aws/credentials /root/.aws/credentials
COPY .aws/config /root/.aws/config

# Instala dependencias del sistema necesarias
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instala Poetry
RUN pip install --upgrade pip && pip install poetry

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /kpi_new/

# Copia los archivos de configuración de Poetry
COPY pyproject.toml poetry.lock /kpi_new/

# Copia el resto de los archivos y directorios, excluyendo lo especificado en .dockerignore
COPY . /kpi_new/

# Instala las dependencias usando Poetry
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# Comando para mantener el contenedor en ejecución (puedes ajustarlo según tus necesidades)
CMD ["tail", "-f", "/dev/null"]
