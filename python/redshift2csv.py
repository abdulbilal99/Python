# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 15:54:58 2020

@author: Abdul Bilal 
"""
import psycopg2
import psycopg2.extras
import csv
import configparser
import boto3
import sys, getopt
import logging
import os.path
import os
import logging.config, subprocess, pathlib
from datetime import date , datetime

def main(argv):
   try:
      opts, args = getopt.getopt(argv,"r:",["rfile="])
   except getopt.GetoptError:
       sys.exit(2)
   

     
today = datetime.now()
ts = today.strftime('%Y%m%d_%H%M%S')
dt =  today.strftime('%Y%m%d') + '/'
load_date  =  today.strftime('%Y-%m-%d')


pathlib.Path('/data/bilal/logs/bilal_gateway_etl').mkdir(parents=True, exist_ok=True)

pathlib.Path('/data/bilal/bilal_gateway_etl').mkdir(parents=True, exist_ok=True) 

pathlib.Path('/data/bilal/bilal_gateway_etl/tmp').mkdir(parents=True, exist_ok=True)


config = configparser.ConfigParser()
config.read('/opt/bilal/bilal_gateway_etl/cfg/script.cfg')

reportname= str(sys.argv[2])

s3_path= reportname + '_s3_path'
bucket_name= (config.get('CFG','bucket'))
processFolderKey= (config.get('CFG',s3_path))
tmpfilepath= (config.get('CFG','tmpfilepath'))
sql= (config.get('CFG','sql'))
client = boto3.client('s3')
log_dir= (config.get('CFG','log_dir'))


log_file_name= log_dir + reportname + ts +'.log'

logging.config.fileConfig('/opt/bilal/bilal_gateway_etl/cfg/logging.ini', disable_existing_loggers=False, defaults={'logfilename': log_file_name})
logger = logging.getLogger('Rotating Log')
logger.info("The process file is %s" % str(sys.argv[2]))

today = date.today()
d1 = today.strftime("%d%m%Y")
dt =  d1 + '/'


for line in open('/opt/bilal/redshift/conf/redshift-connection.properties'):
    if "redshift.jdbc.url" in line:
        hostpre=line.split("=",1)[1]
        hostF=hostpre.split('/',2)[2]
        portpre=hostF.split('/')[0]
    elif "redshift.user" in line:
        user=line.split("=",1)[1]
    elif "redshift.password" in line:
        password=line.split("=",1)[1]

host= os.getenv('RS_URI')
port= os.getenv('RS_PORT')
dbname= os.getenv('RS_DB_NAME')


con=psycopg2.connect(dbname= dbname.strip(), host=host.strip(), port= port.strip(), user= user.strip(), password= password.strip())
sqlpath = sql + reportname +'.sql'
scriptFile = open(sqlpath,'r')
script = scriptFile.read()
logger.info(sqlpath)


tmpFile= tmpfilepath + reportname + '.csv'

email_subject_suc = "JOB ALERT Gateway ETL reports " + reportname + " Report " + load_date + " - COMPLETED"   
email_subject_fail = "JOB ALERT Gateway ETL reports " + reportname + " Report " + load_date + " - Failed" 
email_to = config.get('CFG', 'mail')  

logger.info("Email Subject for Success script: %s" % email_subject_suc)
logger.info("Email Subject for Failure script: %s" % email_subject_fail)
logger.info("Email To: %s" % email_to)

def send_message(email_to, email_subject, body):        
    try:
       process = subprocess.Popen(['mail', '-s', email_subject, email_to], stdin=subprocess.PIPE)
    except Exception as error:
        logger.info("Unable to send email...%s" % error)
    process.communicate(body)

def __removeProcessedTmpFile__(filePath):
   if os.path.exists(filePath):
      os.remove(filePath)
      logger.info("Removed the file %s" % filePath)     
   else:
      logger.info("Sorry, file %s does not exist." % filePath)   
__removeProcessedTmpFile__(tmpFile)

cur = con.cursor()

cur.execute(script)

def ResultIter(cur, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
         results = cur.fetchmany(arraysize)
         if not results:        
            break
         for result in results:
             yield result

logger.info("File Creation START")
try :
   with open(tmpFile, 'w', newline='', encoding='utf-8-sig') as file:
    writer = csv.writer(file, lineterminator = '\n')
    column_names1 = [i[0] for i in cur.description]
    column_names = [sub.replace('_', ' ') for sub in column_names1] 
    writer.writerow(column_names)
    #writer.writerows(cur.fetchall())
    for tup in ResultIter(cur):
      writer.writerow(tup)
    else:
      logger.info("Fetchmany many rows finished!")
    file.close()
except Exception as e:
    logger.error("OOps: File creation failed : %s" % e)
    err_msg = """Hello!\n\n\tERROR occured while creating csv file""" + reportname + """ on """ + load_date
    send_message(email_to, email_subject_fail, err_msg.encode('utf-8'))
    sys.exit(1)


logger.info(" File creation - COMPLETE !!!")	

logger.info(tmpFile)
logger.info(bucket_name)
logger.info(processFolderKey)

try :
    client.upload_file(tmpFile, bucket_name, processFolderKey + reportname + '.csv')
    client.upload_file(tmpFile, bucket_name, 'archive_Reports/' + processFolderKey + dt + reportname + '.csv')
    logger.info("Completed files copying to S3 Bucket !!!")
    msg = """Hello!\n\n\tReport Name """ + reportname + """ exists and data loaded on """ + load_date + """ to S3.\n\nRegards\nbilal Team"""
    send_message(email_to, email_subject_suc, msg.encode("utf-8"))
except Exception as e:
    logger.info ("Failed copying file to S3 Bucket : %s" % e)
    err_msg = """Hello!\n\n\tERROR occured while copying file to S3 Bucket""" + reportname + """ on """ + load_date
    send_message(email_to, email_subject_fail, err_msg.encode('utf-8'))
    sys.exit(1)



logger.info("Program completed")

if __name__ == "__main__":
   main(sys.argv[1:])


