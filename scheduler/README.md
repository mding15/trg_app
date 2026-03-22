# Account Scheduler

This scheduler is specifically designed to handle "track over time" functionality for accounts, automatically executing calculations every Saturday at 0:00.

## Features

- **Fixed Weekly Schedule**: Processes all tracked accounts every Saturday at 0:00
- **Resource Efficient**: Runs only once weekly, greatly saving server resources
- **Simple Logic**: No complex scheduling logic needed - all accounts processed weekly
- **Email Notifications**: Sends email notifications after processing completion
- **Error Handling**: Comprehensive error handling and logging

## How It Works

- Scheduler runs every Saturday at 0:00 via cron job
- Processes all accounts in the `account` table
- Uses current date (Saturday) for all calculations
- No need for `next_run_time` field - fixed weekly schedule

## Usage

### Start the Scheduler

```bash
cd E:\TRG Project\VaR\trgapp\scheduler
python start_account_scheduler.py
```

### Use in Code

```python
from scheduler.account_scheduler import start_scheduler, stop_scheduler

# Start the scheduler
start_scheduler()

# Stop the scheduler
stop_scheduler()
```

## Processing Flow

1. **Get All Tracked Accounts**: Query all accounts from `account` table
2. **Process Accounts**: Call `process.process_account(account_id, current_date)`
3. **Send Notifications**: Send email notifications after processing completion

## Configuration

- **Run Frequency**: Once weekly on Saturday at 0:00
- **Cron Job**: `0 0 * * 6 python /path/to/scheduler/start_account_scheduler.py`
- **Log File**: `account_scheduler.log`

## Database Fields

The `account` table contains:
- `account_id` (int): Primary key
- `account_name` (varchar): Account name
- `owner_id` (int): User ID who owns the account
- `client_id` (int): Client ID
- `create_time` (timestamp): When account was created

## Notes

- Scheduler runs once weekly and exits automatically
- Use cron job to start automatically every Saturday at 0:00
- All operations have detailed logging
- Errors do not affect processing of other accounts
- Fixed weekly schedule - no complex timing logic needed
