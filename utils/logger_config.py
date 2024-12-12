 # utils/logger_config.py

# logger_config.py
import logging

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,  # Nivel de registro
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato
    )
    logger = logging.getLogger(__name__)
    return logger

# Llamada inicial para configurar
setup_logger()
