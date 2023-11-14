from app_data import app
import config.logs as logs
from config.settings import ProjectSettings


settings = ProjectSettings()
logger = logs.get_logger(__name__)

# initialise settings
logger.info("Initializing ...")
settings.validate_data_integrity()
app.execute()
logger.info("Execution Completed ...")
