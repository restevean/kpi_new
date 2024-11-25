import subprocess

def execute_processes():
    processes = [
        "/src/est_gru_ane.py",
        "/src/otro_script.py"
    ]

    for process in processes:
        try:
            print(f"Ejecutando: {process}")
            # Ejecutar cada proceso y esperar a que termine
            subprocess.run(["python3", process], check=True)
            print(f"Finalizado: {process}")
        except subprocess.CalledProcessError as e:
            print(f"Error al ejecutar {process}: {e}")
            break  # Detener el ciclo si un proceso falla
        except Exception as e:
            print(f"Error inesperado en {process}: {e}")
            break

if __name__ == "__main__":
    execute_processes()
