# routers/tasks.py

from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from app.models import TaskExecution, ScheduledTask
from app.utils import templates, write_log  # Importar write_log
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.interval import IntervalTrigger
import logging
import os
from sqlalchemy.orm import Session
from app.database import get_db
from app.scheduler import scheduler, execute_task, add_task_to_scheduler
from pydantic import BaseModel, Field, validator
from datetime import datetime
import asyncio
import json
from typing import Dict

# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Agregar un manejador de logs a la consola
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

router = APIRouter()

@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "title": "Inicio"})

@router.get("/add-task", response_class=HTMLResponse)
async def get_add_task_form(request: Request):
    return templates.TemplateResponse("add_task.html", {"request": request})

# Modelo de datos para recibir los datos del formulario
class TaskData(BaseModel):
    name: str
    script: str
    days: int = Field(default=0, ge=0)
    hours: int = Field(default=0, ge=0)
    minutes: int = Field(default=0, ge=0)
    seconds: int = Field(default=0, ge=0)
    start_date: str  # Nuevo campo para la fecha (formato 'YYYY-MM-DD')
    start_time: str  # Nuevo campo para la hora (formato 'HH:MM')
    active: bool
    task_id: int | None = None  # Opcional

    @validator('start_date', 'start_time')
    def validate_datetime(cls, v):
        if not v:
            raise ValueError('Debe proporcionar una fecha y hora de inicio')
        return v

    def get_start_datetime(self):
        return datetime.strptime(f"{self.start_date} {self.start_time}", "%Y-%m-%d %H:%M")

@router.post("/add-task")
async def add_task(task_data: TaskData, db: Session = Depends(get_db)):
    logger.info("Datos recibidos para procesar: %s", task_data.dict())

    interval_value = (
        task_data.days * 86400 + task_data.hours * 3600 +
        task_data.minutes * 60 + task_data.seconds
    )
    if interval_value <= 0:
        raise HTTPException(
            status_code=400,
            detail="La periodicidad debe ser mayor que cero."
        )

    # Obtener start_datetime
    try:
        start_datetime = task_data.get_start_datetime()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Si `task_id` existe, actualizar la tarea
    if task_data.task_id:
        task = db.query(ScheduledTask).filter(ScheduledTask.id == task_data.task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail="Tarea no encontrada")

        task.name = task_data.name
        task.script_path = task_data.script
        task.interval_type = "seconds"
        task.interval_value = interval_value
        task.start_datetime = start_datetime
        task.is_active = task_data.active
        db.commit()

        # Si la tarea está activa, reprográmala en el programador
        if task.is_active:
            add_task_to_scheduler(task)
        else:
            # Si la tarea está desactivada, eliminarla del scheduler
            try:
                scheduler.remove_job(f"task_{task.id}")
            except JobLookupError:
                pass

        # Registrar la actualización en el log
        write_log(
            task_name=task.name,
            start_datetime=start_datetime.strftime("%Y-%m-%d %H:%M"),
            end_time=datetime.now().strftime("%H:%M:%S"),
            status="success: tarea actualizada"
        )

        return {"message": f"Tarea '{task_data.name}' actualizada correctamente."}

    # Si no existe `task_id`, crear nueva tarea
    new_task = ScheduledTask(
        name=task_data.name,
        script_path=task_data.script,
        interval_type="seconds",
        interval_value=interval_value,
        start_datetime=start_datetime,
        is_active=task_data.active,
    )
    db.add(new_task)
    db.commit()

    # Si la nueva tarea está activa, agrégala al programador
    if new_task.is_active:
        add_task_to_scheduler(new_task)

    # Registrar la creación en el log
    write_log(
        task_name=new_task.name,
        start_datetime=start_datetime.strftime("%Y-%m-%d %H:%M"),
        end_time=datetime.now().strftime("%H:%M:%S"),
        status="success: tarea añadida"
    )

    return {"message": f"Tarea '{task_data.name}' añadida correctamente."}

@router.get("/history", response_class=HTMLResponse)
async def get_history(request: Request, db: Session = Depends(get_db)):
    # Consultar las ejecuciones registradas en la base de datos
    executions = db.query(TaskExecution).order_by(TaskExecution.executed_at.desc()).all()
    return templates.TemplateResponse("history.html", {"request": request, "executions": executions})

@router.get("/get-tasks")
async def get_tasks(db: Session = Depends(get_db)):
    tasks = db.query(ScheduledTask).all()
    task_list = []
    for task in tasks:
        total_seconds = task.interval_value
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        # Obtener start_date y start_time
        if task.start_datetime:
            start_date = task.start_datetime.strftime("%Y-%m-%d")
            start_time = task.start_datetime.strftime("%H:%M")
        else:
            start_date = ""
            start_time = ""

        task_list.append({
            "id": task.id,
            "name": task.name,
            "script_path": task.script_path,
            "periodicity": {
                "days": days,
                "hours": hours,
                "minutes": minutes,
                "seconds": seconds
            },
            "start_date": start_date,
            "start_time": start_time,
            "is_active": task.is_active,
        })
    return JSONResponse(content=task_list)

class ToggleTaskRequest(BaseModel):
    is_active: bool

@router.post("/tasks/{task_id}/toggle")
async def toggle_task(task_id: int, toggle_request: ToggleTaskRequest, db: Session = Depends(get_db)):
    is_active = toggle_request.is_active
    # Obtener la tarea de la base de datos
    task = db.query(ScheduledTask).filter(ScheduledTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")

    task.is_active = is_active
    db.commit()

    job_id = f"task_{task.id}"

    if is_active:
        # Configurar la periodicidad correctamente
        interval_kwargs = {}
        if task.interval_type == "days":
            interval_kwargs["days"] = task.interval_value
        elif task.interval_type == "hours":
            interval_kwargs["hours"] = task.interval_value
        elif task.interval_type == "minutes":
            interval_kwargs["minutes"] = task.interval_value
        elif task.interval_type == "seconds":
            interval_kwargs["seconds"] = task.interval_value

        # Agregar el trabajo al programador con start_datetime
        scheduler.add_job(
            execute_task,
            trigger=IntervalTrigger(**interval_kwargs, start_date=task.start_datetime),
            args=[task.id],
            id=job_id,
            replace_existing=True,
        )
        status_msg = "success: tarea activada"
    else:
        # Desactivar la tarea si está desmarcada
        try:
            scheduler.remove_job(job_id)
            status_msg = "success: tarea desactivada"
        except JobLookupError:
            status_msg = "warning: tarea no encontrada en scheduler"

    # Registrar el cambio de estado en el log
    write_log(
        task_name=task.name,
        start_datetime=task.start_datetime.strftime("%Y-%m-%d %H:%M"),
        end_time=datetime.now().strftime("%H:%M:%S"),
        status=status_msg
    )

    return {"message": f"Tarea {'activada' if is_active else 'desactivada'} correctamente"}

from fastapi import Response, status

@router.delete("/tasks/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_task(task_id: int, db: Session = Depends(get_db)):
    # Obtener la tarea de la base de datos
    task = db.query(ScheduledTask).filter(ScheduledTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")

    task_name = task.name
    start_datetime = task.start_datetime.strftime("%Y-%m-%d %H:%M") if task.start_datetime else "Unknown"

    # Eliminar la tarea de la base de datos
    db.delete(task)
    db.commit()

    # Eliminar la tarea del scheduler si está activa
    job_id = f"task_{task.id}"
    try:
        scheduler.remove_job(job_id)
        logger.info(f"Tarea {task_id} eliminada del scheduler.")
    except JobLookupError:
        logger.warning(f"Tarea {task_id} no estaba en el scheduler.")

    # Registrar la eliminación en el log
    write_log(
        task_name=task_name,
        start_datetime=start_datetime,
        end_time=datetime.now().strftime("%H:%M:%S"),
        status="success: tarea eliminada"
    )

    return Response(status_code=status.HTTP_204_NO_CONTENT)

from fastapi import Query  # Asegúrate de importar Query


@router.get("/get-logs")
async def get_logs(
        task_name: str = Query(None, description="Filtrar por nombre de la tarea"),
        status: str = Query(None, description="Filtrar por estado (success o error)"),
        start_date: str = Query(None, description="Filtrar por fecha de inicio desde (YYYY-MM-DD)"),
        date: str = Query(None, description="Fecha en formato YYMMDD")  # Parámetro opcional
):
    """
    Obtiene el contenido del archivo de log para una fecha específica.
    :param date: Fecha en formato YYMMDD (opcional)
    """
    if date:
        log_filename = f"app_scheduler_{date}.txt"
    else:
        # Usar la fecha actual si no se proporciona 'date'
        log_filename = datetime.now().strftime("app_scheduler_%y%m%d.txt")

    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
    log_filepath = os.path.join(logs_dir, log_filename)

    if not os.path.exists(log_filepath):
        raise HTTPException(status_code=404, detail="Archivo de log no encontrado.")

    logs = []
    try:
        with open(log_filepath, "r", encoding="utf-8") as log_file:
            for line in log_file:
                parts = line.strip().split(', ')
                if len(parts) < 4:
                    continue  # Ignorar líneas mal formateadas

                task_name_log = parts[0]
                start_datetime = parts[1]
                end_time = parts[2]
                status_action = ', '.join(parts[3:])  # En caso de que haya más comas

                # Separar estado y acción si existe ': '
                if ': ' in status_action:
                    status_log, action = status_action.split(': ', 1)
                else:
                    status_log = status_action
                    action = ""

                # Aplicar filtros
                if task_name and task_name.lower() not in task_name_log.lower():
                    continue
                if status:
                    if status.lower() == "success" and not status_log.lower().startswith("success"):
                        continue
                    elif status.lower() == "error" and not status_log.lower().startswith("error"):
                        continue
                if start_date:
                    try:
                        log_start_date = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
                        filter_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                        if log_start_date < filter_start_date:
                            continue
                    except ValueError:
                        continue  # Ignorar si la fecha no está en el formato esperado

                log_entry = {
                    "task_name": task_name_log,
                    "start_datetime": start_datetime,
                    "end_time": end_time,
                    "status": status_log,
                    "action": action
                }
                logs.append(log_entry)

        return JSONResponse(content=logs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer el archivo de logs: {str(e)}")
@router.get("/get-all-logs")
async def get_all_logs():
    """
    Obtiene todos los logs disponibles.
    """
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'logs')
    if not os.path.exists(logs_dir):
        raise HTTPException(status_code=404, detail="Directorio de logs no encontrado.")

    log_files = [f for f in os.listdir(logs_dir) if f.startswith('app_scheduler_') and f.endswith('.txt')]
    all_logs = {}

    for log_file in log_files:
        date = log_file[len('app_scheduler_'):-len('.txt')]  # Extraer YYMMDD
        log_filepath = os.path.join(logs_dir, log_file)
        with open(log_filepath, 'r', encoding='utf-8') as f:
            lines = f.read().strip().split('\n')
            entries = []
            for line in lines:
                if not line.strip():
                    continue
                parts = line.split(', ')
                if len(parts) != 4:
                    continue  # Ignorar líneas mal formateadas
                entry = {
                    "name": parts[0],
                    "start_datetime": parts[1],
                    "end_time": parts[2],
                    "status": parts[3]
                }
                entries.append(entry)
            all_logs[date] = entries

    return all_logs


@router.get("/stream-logs")
async def stream_logs(request: Request, date: str = Query(..., regex="^\d{6}$")):
    """
    Stream de logs en tiempo real para una fecha específica usando SSE.
    :param request: Objeto de solicitud de FastAPI.
    :param date: Fecha en formato YYMMDD
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(current_dir, '..', 'logs')
    log_filename = f"app_scheduler_{date}.txt"
    log_filepath = os.path.join(logs_dir, log_filename)

    if not os.path.exists(log_filepath):
        logger.error(f"Archivo de log no encontrado: {log_filepath}")
        raise HTTPException(status_code=404, detail="Archivo de log no encontrado.")

    async def event_generator():
        """
        Generador asincrónico que lee nuevas líneas del archivo de log y las envía como eventos SSE.
        """
        with open(log_filepath, 'r', encoding='utf-8') as f:
            # Ir al final del archivo para empezar a escuchar nuevas entradas
            f.seek(0, os.SEEK_END)
            while True:
                # Verificar si el cliente aún está conectado
                if await request.is_disconnected():
                    logger.info("Cliente desconectado del stream de logs.")
                    break

                line = f.readline()
                if not line:
                    await asyncio.sleep(0.5)  # Esperar antes de intentar leer de nuevo
                    continue

                parts = line.strip().split(', ')
                if len(parts) < 4:
                    logger.warning(f"Línea mal formateada ignorada: {line.strip()}")
                    continue  # Ignorar líneas mal formateadas

                task_name = parts[0]
                start_datetime = parts[1]
                end_time = parts[2]
                status_action = ', '.join(parts[3:])  # En caso de que haya más comas en el status

                # Separar estado y acción si existe ':'
                if ': ' in status_action:
                    status, action = status_action.split(': ', 1)
                else:
                    status = status_action
                    action = ""

                log_entry = {
                    "task_name": task_name,
                    "start_datetime": start_datetime,
                    "end_time": end_time,
                    "status": status,
                    "action": action
                }

                # Enviar el evento SSE
                yield f"data: {json.dumps(log_entry)}\n\n"

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

    return StreamingResponse(event_generator(), headers=headers)