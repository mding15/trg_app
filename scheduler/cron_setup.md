# Cron Job Setup Instructions

## Set up automatic weekly execution of the scheduler every Saturday at 0:00

### 1. Edit crontab
```bash
crontab -e
```

### 2. Add the following line
```bash
# Run account scheduler every Saturday at 0:00 (midnight)
0 0 * * 6 cd /path/to/TRG\ Project/VaR/trgapp/scheduler && python start_account_scheduler.py >> /var/log/account_scheduler.log 2>&1
```

### 3. Save and exit
- In vim: Press `Esc`, then type `:wq`
- In nano: Press `Ctrl+X`, then press `Y`, then press `Enter`

### 4. Verify cron job was set successfully
```bash
crontab -l
```

### 5. Check if cron service is running
```bash
# Ubuntu/Debian
sudo systemctl status cron

# CentOS/RHEL
sudo systemctl status crond
```

## Log File Locations

- **Scheduler Log**: `/var/log/account_scheduler.log`
- **Application Log**: `account_scheduler.log` (in scheduler directory)

## Manual Testing

Before setting up the cron job, you can test manually:

```bash
cd /path/to/TRG\ Project/VaR/trgapp/scheduler
python start_account_scheduler.py
```

## EC2 Server Deployment

### 1. Upload code to EC2
```bash
# Upload the entire project to EC2
scp -r /path/to/TRG\ Project/VaR/trgapp ec2-user@your-ec2-ip:/home/ec2-user/
```

### 2. Set up Python environment on EC2
```bash
# SSH into EC2
ssh ec2-user@your-ec2-ip

# Install Python dependencies
cd /home/ec2-user/trgapp
pip install -r requirements.txt
```

### 3. Set up cron job on EC2
```bash
# Edit crontab on EC2
crontab -e

# Add the following line (adjust path as needed)
0 0 * * 6 cd /home/ec2-user/trgapp/scheduler && python start_account_scheduler.py >> /var/log/account_scheduler.log 2>&1
```

### 4. Verify cron job on EC2
```bash
# Check if cron job was set
crontab -l

# Check cron service status
sudo systemctl status cron
```

## Important Notes

1. **EC2 Path**: Update the path in cron job to match your EC2 deployment location
2. **Python Environment**: Ensure Python and all dependencies are installed on EC2
3. **Permissions**: Ensure sufficient permissions to write log files
4. **Database Connection**: Ensure EC2 can connect to your database
5. **Manual Testing**: Test manually on EC2 before setting up cron job
6. **Log Monitoring**: Monitor log files to ensure scheduler runs normally
7. **Time Zone**: Ensure EC2 server time zone is correct for Saturday 0:00 execution
