# dashboard daily process cron job
0 18 * * 1-5  cd /home/ec2-user/api && .venv/bin/python dashboard/dashboard_process.py