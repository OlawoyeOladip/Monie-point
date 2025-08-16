from src.anomaly_detection.utils.logging_utils import setup_logging
import logging
from src.anomaly_detection.pipeline.stage_01_ingestion import DataIngestionTrainingPipeline
from src.anomaly_detection.pipeline.stage_02_data_validation import DataValidationTrainingPipeline

# ----------- Data Ingestion Stage -----------
STAGE_NAME = "Data Ingestion Stage"
setup_logging("stage1_ingestion.log")  # GLOBAL logging for this stage

try:
    logging.info(f">>>>> {STAGE_NAME} started <<<<<<")
    pipeline = DataIngestionTrainingPipeline()
    pipeline.main()
    logging.info(f">>>>> {STAGE_NAME} completed <<<<<<\n\nX============X")
except Exception as e:
    logging.error(f"Error in {STAGE_NAME}: {e}")
    raise e

# ----------- Data Validation Stage -----------
STAGE_NAME = "Data Validation Stage"
setup_logging("stage2_data_validation.log")  # GLOBAL logging for this stage

try:
    logging.info(f">>>>> {STAGE_NAME} started <<<<<<")
    obj = DataValidationTrainingPipeline()
    obj.main()
    logging.info(f">>>>> {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
    logging.error(f"Error in {STAGE_NAME}: {e}")
    raise e
