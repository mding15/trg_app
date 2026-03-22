# -*- coding: utf-8 -*-
"""
Account Scheduler for track over time portfolios
Created for automatic weekly processing of account-based portfolios
Runs every Saturday at 0:00 via cron job
"""

import logging
from datetime import datetime
from typing import List, Dict
import threading

from database import model_aux
from process.process import process_account
from api import email2

logger = logging.getLogger(__name__)


class AccountScheduler:
    """Scheduler for handling weekly account portfolio processing"""
    
    def __init__(self):
        self.running = False
        self.thread = None
        
    def start(self):
        """Start the scheduler - runs once and exits (designed for cron job)"""
        if self.running:
            logger.warning("Scheduler is already running")
            return
            
        self.running = True
        logger.info("Account scheduler started - processing all tracked accounts")
        
        # Run once and exit (designed for cron job)
        try:
            self._process_all_tracked_accounts()
        except Exception as e:
            logger.error(f"Error in scheduler execution: {e}")
        finally:
            self.running = False
            logger.info("Account scheduler completed")
        
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        logger.info("Account scheduler stopped")
                
    def _process_all_tracked_accounts(self):
        """Process all tracked accounts (fixed weekly schedule)"""
        try:
            # Get all tracked accounts
            accounts = self._get_all_tracked_accounts()
            
            if not accounts:
                logger.info("No tracked accounts found")
                return
                
            logger.info(f"Found {len(accounts)} tracked accounts to process")
            
            for account in accounts:
                try:
                    self._process_account(account)
                except Exception as e:
                    logger.error(f"Error processing account {account['account_id']}: {e}")
                    
        except Exception as e:
            logger.error(f"Error getting tracked accounts: {e}")
            
    def _get_all_tracked_accounts(self) -> List[Dict]:
        """Get all tracked accounts (no next_run_time needed)"""
        try:
            # Get all accounts (they are all tracked by default)
            sql = """
            SELECT 
                account_id,
                account_name,
                owner_id,
                client_id,
                create_time
            FROM account
            ORDER BY account_id ASC
            """
            
            from database import db_utils
            df = db_utils.get_sql_df(sql)
            
            accounts = []
            for _, row in df.iterrows():
                accounts.append({
                    'account_id': row['account_id'],
                    'account_name': row['account_name'],
                    'owner_id': row['owner_id'],
                    'client_id': row['client_id'],
                    'create_time': row['create_time']
                })
                
            return accounts
            
        except Exception as e:
            logger.error(f"Error getting tracked accounts: {e}")
            return []
            
    def _process_account(self, account: Dict):
        """Process a single account"""
        try:
            account_id = account['account_id']
            account_name = account['account_name']
            
            logger.info(f"Processing account {account_id} ({account_name})")
            
            # Process the account using the existing process_account function
            # Use close of business date (Friday) for processing
            from utils.date_utils import get_cob
            as_of_date = get_cob()
            process_account(account_id, as_of_date)
            
            # Send success notification
            self._send_notification(account, success=True)
            
            logger.info(f"Successfully processed account {account_id}")
            
        except Exception as e:
            error_msg = f"Weekly auto-run failed: {str(e)}"
            logger.error(f"Error processing account {account['account_id']}: {e}")
            
            # Send error notification
            self._send_notification(account, success=False, error_message=error_msg)
            
    def _send_notification(self, account: Dict, success: bool, error_message: str = None):
        """Send notification about account processing result"""
        try:
            if success:
                subject = f"Account {account['account_id']} Weekly Auto-Run Completed"
                message = f"Account '{account['account_name']}' weekly processing completed successfully."
            else:
                subject = f"Account {account['account_id']} Weekly Auto-Run Failed"
                message = f"Account '{account['account_name']}' weekly processing failed: {error_message}"
            
            # Get user info
            user = model_aux.get_user_by_id(account['owner_id'])
            if user:
                email2.send_portfolio_status_notification(
                    None,  # No port_id for account-based processing
                    account['account_name'],
                    user.username,
                    user.client.client_name,
                    subject,
                    message
                )
                
        except Exception as e:
            logger.error(f"Error sending notification for account {account['account_id']}: {e}")
            


# Global scheduler instance
scheduler = AccountScheduler()


def start_scheduler():
    """Start the account scheduler"""
    scheduler.start()


def stop_scheduler():
    """Stop the account scheduler"""
    scheduler.stop()


