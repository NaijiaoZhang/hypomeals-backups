import boto3
from botocore.client import Config
import subprocess
import urllib.request
import schedule
import time
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


ACCESS_KEY_ID = 'access_key'
ACCESS_SECRET_KEY = 'secret_key'
BUCKET_NAME = 'hypomeals-backup'
counter = 0

def job(t):
	# Date
	try:
		global counter
		now = datetime.datetime.now()
		today_date = now.strftime("%Y-%m-%d")
		# Get url from heroku backups
		cmd = [ 'heroku', 'pg:backups:url', '--app', 'hypomeals' ]
		output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
		decoded_output = output.decode('utf-8')
		response = urllib.request.urlretrieve(decoded_output,"dailydump")

		# Open and Store in S3
		data = open('dailydump', 'rb')

		s3 = boto3.resource(
		    's3',
		    aws_access_key_id=ACCESS_KEY_ID,
		    aws_secret_access_key=ACCESS_SECRET_KEY,
		    config=Config(signature_version='s3v4')
		)
		if(counter % 7 != 0):
			s3.Bucket(BUCKET_NAME).put_object(Key='daily/'+today_date+"_"+str(counter), Body=data)
			counter = counter + 1
		elif(counter % 30 == 0): 
			s3.Bucket(BUCKET_NAME).put_object(Key='monthly/'+today_date+"_"+str(counter), Body=data)
			counter = counter + 1
		elif(count % 356 == 0):
			s3.Bucket(BUCKET_NAME).put_object(Key='yearly/'+today_date+"_"+str(counter), Body=data)
			counter = 0


		# Email 
		fromaddr = "hypomeals@gmail.com"
		toaddr = "dhz@duke.edu"

		send_email = "send_email"
		send_pw = "send_pw"

		msg = MIMEMultipart()
		msg['From'] = fromaddr
		msg['To'] = toaddr
		msg['Subject'] = "BACKUPS SUCCESS "+today_date
		body = "Successful backup on : "+today_date
		msg.attach(MIMEText(body, 'plain'))

		text = msg.as_string()

		mail = smtplib.SMTP('smtp.gmail.com',587)

		mail.ehlo()

		mail.starttls()

		mail.login(send_email,send_pw)

		mail.sendmail(send_email,toaddr,text)

		mail.close()
	except: 
		# Email Failure
		fromaddr = "hypomeals@gmail.com"
		toaddr = "dhz@duke.edu"

		msg = MIMEMultipart()
		msg['From'] = fromaddr
		msg['To'] = toaddr
		msg['Subject'] = "BACKUPS FAILED"
		body = "FAILURE ACTION REQUIRED: BACKUPS FAILED"
		msg.attach(MIMEText(body, 'plain'))

		text = msg.as_string()

		mail = smtplib.SMTP('smtp.gmail.com',587)

		mail.ehlo()

		mail.starttls()

		mail.login(send_email,send_pw)

		mail.sendmail(send_email,toaddr,text)

		mail.close()

schedule.every().day.at("01:15").do(job,'It is 01:00')
# schedule.every().hour.at(":25").do(job,'It is 01:00')


while True: 
	schedule.run_pending()
	print("Still running: "+datetime.datetime.now().strftime("%Y-%m-%d"))
	time.sleep(60) # wait one minute

