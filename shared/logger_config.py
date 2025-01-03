import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logger(service_name: str, component_name: str, append: bool = True):
    """
    Set up logger with both file and console handlers

    Args:
        service_name: Name of the service (e.g., 'checker', 'mapper', 'kerchunker')
        component_name: Name of the component within the service (e.g., 'noaa', 'db_tools')
        append: If True, append to existing log file. If False, overwrite it. Default True.
    """

    # Create logs directory if it doesn't exist
    logs_dir = Path("/logs")
    logs_dir.mkdir(exist_ok=True)

    # Create logger
    logger_name = f"{service_name}.{component_name}"
    logger = logging.getLogger(logger_name)

    # Prevent adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s | %(name)s:%(lineno)d | %(levelname)s - %(message)s"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s | %(name)s:%(lineno)d | %(levelname)s - %(message)s"
    )

    # Regular file handler
    log_file = f"{service_name}_{component_name}.log"
    file_handler = logging.FileHandler(f"/logs/{log_file}", mode="a" if append else "w")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # Error file handler
    error_log_file = f"{service_name}_{component_name}_errors.log"
    error_file_handler = logging.FileHandler(
        f"/logs/{error_log_file}", mode="a" if append else "w"
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(error_file_handler)
    logger.addHandler(console_handler)

    return logger
