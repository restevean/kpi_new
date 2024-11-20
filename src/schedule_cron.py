import subprocess


def schedule_cron_job():
    # Ruta completa del script controlador
    script_path = "/utils/cron_job.py"

    # Comando cron para ejecutar el controlador cada 5 minutos
    cron_command = f"*/5 * * * * python3 {script_path}"

    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        current_cron_jobs = result.stdout if result.returncode == 0 else ""
        if cron_command in current_cron_jobs:
            print("La tarea ya está programada en cron.")
            return

        updated_cron_jobs = f"{current_cron_jobs.strip()}\n{cron_command}\n"
        process = subprocess.run(["crontab", "-"], input=updated_cron_jobs.strip() + "\n", text=True)
        if process.returncode == 0:
            print("Tarea cron programada con éxito.")
        else:
            print("Hubo un error al programar la tarea cron.")

    except Exception as e:
        print(f"Error al configurar la tarea cron: {e}")


if __name__ == "__main__":
    schedule_cron_job()
