import os
import logging
import sys

def setup_logging(log_filename: str) -> None:
    """
    Set up a global logging configuration for the entire pipeline stage.

    Args:
        log_filename (str): The name of the log file (e.g., "data_ingestion.log").
    """
    # Get the main project directory
    project_root = os.getcwd()  
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)

    # Full path for the log file
    log_file_path = os.path.join(log_dir, log_filename)

    # Clear existing logging handlers (avoid duplicates across multiple calls)
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Configure global logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),  # Log to file
            logging.StreamHandler(sys.stdout)    # Also log to console
        ]
    )
