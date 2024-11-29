# main.py

import logging
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.database import Base, engine
from app.routers import tasks
from app.scheduler import start_scheduler, scheduler
from fastapi import Request
from fastapi.responses import Response

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# logging.basicConfig()
logger = logging.getLogger(__name__)
logging.getLogger("apscheduler").setLevel(logging.DEBUG)

# Crear las tablas en la base de datos
Base.metadata.create_all(bind=engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializar el programador al inicio de la aplicación
    logger.info("Iniciando la aplicación y el programador.")
    start_scheduler()
    yield
    # Detener el programador al cerrar la aplicación
    logger.info("Apagando el programador y la aplicación.")
    scheduler.shutdown()

# Inicializar FastAPI con el ciclo de vida configurado
app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def log_request_data(request: Request, call_next):
    if request.url.path == "/tasks/add-task":
        try:
            body = await request.json()
            logger.info("Datos recibidos en el backend: %s", body)
        except Exception as e:
            logger.error(f"Error leyendo los datos: {e}")

    return await call_next(request)



# Inclir los routers
app.include_router(tasks.router)

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=False)
