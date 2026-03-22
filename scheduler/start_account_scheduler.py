#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Account Scheduler Startup Script
Starts the weekly account portfolio processing scheduler
"""

import sys
import os
import signal
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scheduler.account_scheduler import start_scheduler, stop_scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('account_scheduler.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}. Shutting down scheduler...")
    stop_scheduler()
    sys.exit(0)


def main():
    """Main function to start the account scheduler"""
    try:
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("Starting Account Scheduler...")
        logger.info("This scheduler will process track over time portfolios daily at 1 AM")
        logger.info("Processing accounts with next_run_time <= current time")
        
        # Start the scheduler (runs once and exits)
        start_scheduler()
            
    except Exception as e:
        logger.error(f"Failed to start account scheduler: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
