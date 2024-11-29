# scheduler.py

import subprocess
import logging
import os
from datetime import datetime
import asyncio  # Añadido para manejo de subprocessos asíncronos

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.interval import IntervalTrigger

from app.models import ScheduledTask
from app.database import SessionLocal
from app.utils import write_log

# Configuración de logs
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Agregar un manejador de logs a la consola si aún no lo tienes
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# Crear el programador global
scheduler = AsyncIOScheduler()

async def execute_task(task_id: int):
    """
    Ejecuta la tarea programada y registra su ejecución en los logs.
    """
    db = SessionLocal()
    try:
        task = db.query(ScheduledTask).filter(ScheduledTask.id == task_id).first()
        if not task:
            logger.error(f"Tarea con ID {task_id} no encontrada.")
            write_log(
                task_name=f"Tarea ID {task_id}",
                start_datetime="Unknown",
                end_time=datetime.now().strftime('%H:%M:%S'),
                status="error: tarea no encontrada"
            )
            return

        task_name = task.name
        script_path = task.script_path

        start_time = datetime.now()
        start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Ejecutando tarea: {task_name} (ID: {task_id})")

        try:
            # Ejecutar el script usando subprocess de forma asíncrona
            process = await asyncio.create_subprocess_exec(
                'python', script_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()
            end_time = datetime.now()
            end_str = end_time.strftime('%H:%M:%S')

            if process.returncode == 0:
                status = "success: tarea ejecutada correctamente"
                logger.info(f"Tarea '{task_name}' ejecutada exitosamente.")
                logger.info(f"Salida del script: {stdout.decode().strip()}")
            else:
                status = f"error: {stderr.decode().strip()}"
                logger.error(f"Error al ejecutar la tarea '{task_name}': {stderr.decode().strip()}")

        except Exception as e:
            end_time = datetime.now()
            end_str = end_time.strftime('%H:%M:%S')
            status = f"error: {str(e)}"
            logger.error(f"Error inesperado al ejecutar la tarea '{task_name}': {e}")

        # Registrar la ejecución en el log
        write_log(
            task_name=task_name,
            start_datetime=start_str,
            end_time=end_str,
            status=status
        )

    except Exception as ex:
        logger.exception(f"Excepción inesperada al ejecutar la tarea ID {task_id}: {ex}")
        write_log(
            task_name=f"Tarea ID {task_id}",
            start_datetime="Unknown",
            end_time=datetime.now().strftime('%H:%M:%S'),
            status=f"error: {str(ex)}"
        )
    finally:
        db.close()

def load_scheduled_tasks():
    logger.info("Cargando tareas activas desde la base de datos...")
    db = SessionLocal()
    try:
        tasks = db.query(ScheduledTask).filter(ScheduledTask.is_active == True).all()
        for task in tasks:
            interval_seconds = 0

            # Convertir el intervalo a segundos
            if task.interval_type == "days":
                interval_seconds += task.interval_value * 86400  # Días a segundos
            elif task.interval_type == "hours":
                interval_seconds += task.interval_value * 3600  # Horas a segundos
            elif task.interval_type == "minutes":
                interval_seconds += task.interval_value * 60  # Minutos a segundos
            elif task.interval_type == "seconds":
                interval_seconds += task.interval_value

            # Evitar agregar tareas con intervalos inválidos
            if interval_seconds <= 0:
                logger.warning(f"Tarea '{task.name}' tiene un intervalo inválido. Será omitida.")
                continue

            # Agregar la tarea al programador
            scheduler.add_job(
                execute_task,
                IntervalTrigger(seconds=interval_seconds),
                args=[task.id],  # Pasar task.id en lugar de task.script_path
                id=f"task_{task.id}",
                replace_existing=True,
            )
            logger.info(f"Tarea programada: {task.name} (Intervalo: {interval_seconds} segundos)")
    finally:
        db.close()

def add_task_to_scheduler(task: ScheduledTask):
    """
    Agrega o actualiza una tarea en el programador usando sus propiedades.
    """
    try:
        interval_kwargs = {}
        if task.interval_type == "days":
            interval_kwargs["days"] = task.interval_value
        elif task.interval_type == "hours":
            interval_kwargs["hours"] = task.interval_value
        elif task.interval_type == "minutes":
            interval_kwargs["minutes"] = task.interval_value
        elif task.interval_type == "seconds":
            interval_kwargs["seconds"] = task.interval_value

        scheduler.add_job(
            execute_task,
            IntervalTrigger(**interval_kwargs),
            args=[task.id],  # Pasar task.id en lugar de task.script_path
            id=f"task_{task.id}",
            replace_existing=True,
        )
        logger.info(f"Tarea programada o actualizada: {task.name} (ID: {task.id})")
    except Exception as e:
        logger.error(f"Error al programar la tarea {task.name}: {e}")

def remove_task_from_scheduler(task_id: int):
    """
    Elimina una tarea del programador usando su ID.
    """
    try:
        scheduler.remove_job(f"task_{task_id}")
        logger.info(f"Tarea con ID {task_id} eliminada del programador.")
    except JobLookupError:
        logger.warning(f"No se encontró la tarea con ID {task_id} para eliminar.")
    except Exception as e:
        logger.error(f"Error al eliminar la tarea con ID {task_id}: {e}")

def start_scheduler():
    """
    Inicia el programador y carga las tareas activas desde la base de datos.
    """
    load_scheduled_tasks()
    scheduler.start()
    logger.info("El programador de tareas ha comenzado.")
