set PEM=C:\Users\mgdin\local\AWS\KeyPairs\prod2.pem
SET FROM_FOLDER=tests/test_hdf.py
scp -i %PEM% %FROM_FOLDER% ec2-user@ec2-54-86-24-102.compute-1.amazonaws.com:/home/ec2-user/api/trgapp/tests


