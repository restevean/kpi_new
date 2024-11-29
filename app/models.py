# models.py

from sqlalchemy import Column, Integer, String, Boolean, DateTime
from app.database import Base
from datetime import datetime, timezone

# Modelo para ejecuciones de tareas
class TaskExecution(Base):
    __tablename__ = "task_executions"
    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String, index=True)
    status = Column(String, default="success")
    error_message = Column(String, nullable=True)
    executed_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at = Column(DateTime, nullable=True)

# Modelo para tareas programadas
class ScheduledTask(Base):
    __tablename__ = "scheduled_tasks"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    script_path = Column(String, nullable=False)
    interval_type = Column(String, nullable=False)  # 'minutes' o 'days'
    interval_value = Column(Integer, nullable=False)
    start_datetime = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    is_active = Column(Boolean, default=False)
