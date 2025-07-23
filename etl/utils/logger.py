import logging
from pathlib import Path
from datetime import datetime

class LoggerConfig:
    _is_configured = False
    _log_path = Path('./logs')
    _log_level = logging.INFO
    _formatter = logging.Formatter('[%(asctime)s]  %(filename)s:%(lineno)d - %(levelname)s - %(message)s')

    @classmethod
    def set_log_path(cls, path: str):
        cls._log_path = Path(path)

    @classmethod
    def set_formatter(cls, format: logging.Formatter):
        cls._formatter = format
    
    @classmethod
    def configure_root_logger(cls):
        if cls._is_configured:
            return

        # Root Logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        cls._log_path.mkdir(parents=True, exist_ok=True)
        filepath = cls._log_path / f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

        # File Logger
        filehandler = logging.FileHandler(filepath)
        filehandler.setLevel(logging.INFO)
        filehandler.setFormatter(cls._formatter)

        # Console Logger
        streamhandler = logging.StreamHandler()
        streamhandler.setLevel(logging.INFO)
        streamhandler.setFormatter(cls._formatter)

        # Add handlers
        logger.addHandler(filehandler)
        logger.addHandler(streamhandler)

        cls._is_configured = True

class Logger(LoggerConfig):
    def __init__(self, name: str):
        self.configure_root_logger()
        self.logger = logging.getLogger(name)

    def __getattr__(self, name):
        return getattr(self.logger, name)