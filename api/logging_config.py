# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 14:10:59 2024

@author: mgdin
"""

# logging_config.py
import logging
import sys
from utils import tools
from trg_config import config

ts = tools.file_ts()
logfile = config['LOG_DIR']  / f"api.{ts}.log"

# Configure the root logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.StreamHandler(sys.stdout),  # Logs to stdout
        logging.FileHandler(logfile)       # Logs to a file
    ]
)

# Create a function to get a logger in each module
def get_logger(name):
    return logging.getLogger(name)

# redirect print() statements to a logger
class PrintToLogger:
    """Redirect print() statements to a logger."""
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message.strip():  # Avoid empty lines
            self.logger.log(self.level, message.strip())

    def flush(self):
        pass  # Needed for compatibility with sys.stdout
        
logger = logging.getLogger("PrintLogger")

# Redirect print() output to logger
if not config['DEBUG']:
    sys.stdout = PrintToLogger(logger)