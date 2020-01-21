from datetime import date

# import os
# import datetime
import subprocess
import smtplib
import time
import socket
import logging


TARGET_ENV="ENV NAME"
KEYSPACE="KEYSPACE NAME"

cqlsh_path='/opt/app/cassandra/bin/cqlsh'
cassandra_nodes='/opt/app/truncate_cron/cassandra_nodes.txt'
nodetool_path='/opt/app/cassandra/bin/nodetool'
truncate_cql='/opt/app/truncate_cron/truncate.cql'
log_file='/home/schalla/truncate_cron/truncate_cron.log'

#time to wait to run cleanup(seconds)
TIME_TO_WAIT=10
# Flag to enable/disable email notification
sendEmailNotification = True
SMTP_HOST='SMTP_HOSTNAME'

# Email sender
sender = 'senderemail@email.com'

# List of email receivers
receivers = ['receiveremail@email.com']


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def sendEmailNotification(SUBJECT, TEXT):
    # print('Subject: ', SUBJECT)

    BODY = '\r\n'.join([
        'To: %s' % ','.join(receivers),
        'From: %s' % sender,
        'Subject: %s' % SUBJECT,
        '',
        TEXT
    ])
    try:
        smtpObj = smtplib.SMTP(SMTP_HOST)
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.sendmail(sender, receivers, BODY)
        logger.debug('Successfully sent email')
    except:
        logger.debug('sendEmailNotification Error: unable to send email')
    finally:
        smtpObj.quit()


def truncateTable(table_name):
    logger.debug("Inside truncateTable()")
    logger.debug("Table to truncate: {}".format(table_name))
    CQL_STATEMENT='DESCRIBE TABLE ' + KEYSPACE + '."' + table_name + '";'
    logger.debug("CQL_STATEMENT: {}".format(CQL_STATEMENT))

    with open(truncate_cql, 'w') as file:
        file.write(CQL_STATEMENT)

    try:
        logger.debug("Inside subprocess call :")
        call_list = []
        call_list.append(cqlsh_path)
        call_list.append(socket.gethostname())
        call_list.append('--username=cassdba')
        call_list.append('--password=0z64R1a46S')
        call_list.append('-f')
        call_list.append('truncate.cql')

        subprocess.call(call_list)
    except:
        event_subject = "Error running cql script on via cron to truncate " + table_name + " on " + TARGET_ENV
        event_body = "Unable run cql script via cron to truncate " + table_name + "Please check logs"
        sendEmailNotification(event_subject, event_body)
    else:
        # Send Email Notification
        event_subject = "Truncated " + table_name +  " on Cassandra Archive Cluster - " + TARGET_ENV
        event_body = event_subject
        if sendEmailNotification:
            sendEmailNotification(event_subject, event_body)
    finally:
        logger.debug("Pausing for {} seconds".format(TIME_TO_WAIT))
        time.sleep(TIME_TO_WAIT)
        run_cleanup()

def run_cleanup():
    with open(cassandra_nodes, 'r') as file:
         servers=file.readlines()
         call_list = []
         for server in servers:
             logger.debug("Cleanup running on {}".format(server.strip("\n")))
             logger.info("Cleanup running on {}".format(server.strip("\n")))
             call_list.append(nodetool_path)
             call_list.append("-h")
             call_list.append(server.strip("\n"))
             call_list.append("-p")
             call_list.append("7199")
             call_list.append("clearsnapshot")

             logger.debug("Run nodetool cleanup on {}".format(server.strip("\n")))
             subprocess.call(call_list)
             del call_list[:]

def getArchiveTableName():
        today = date.today()

        CURRENT_YEAR = today.strftime("%G")
        CURRENT_WEEK = today.strftime("%V")
        CURRENT_MONTH = today.strftime("%m")


        # TEST
        # CURRENT_MONTH="02"

        MONTHS = ["01","02","03","04","05","06","07","08","09","10","11","12"]

        if int(CURRENT_MONTH) < 5:
            if int(CURRENT_MONTH) == 1:
                PREVIOUS_YEAR = int(CURRENT_YEAR) - 1
                logger.debug("Previous YEAR: {}".format(PREVIOUS_YEAR))
                MONTH_TABLE_TO_TRUNCATE = MONTHS[-1:-5:-1][-1]
                logger.debug("Month Table to truncate: {}".format(MONTH_TABLE_TO_TRUNCATE))
                TABLE_NAME= "DEPLOYED_MESSAGES_V2_" + str(PREVIOUS_YEAR) + "M" + str(MONTH_TABLE_TO_TRUNCATE)
                logger.debug("Table to truncate: {}".format(TABLE_NAME))
                return TABLE_NAME

            if int(CURRENT_MONTH) == 2:
                PREVIOUS_YEAR = int(CURRENT_YEAR) - 1
                logger.debug("Previous YEAR: {}".format(PREVIOUS_YEAR))
                MONTH_TABLE_TO_TRUNCATE = MONTHS[-1:-4:-1][-1]
                logger.debug("Month Table to truncate: {}".format(MONTH_TABLE_TO_TRUNCATE))
                TABLE_NAME= "DEPLOYED_MESSAGES_V2_" + str(PREVIOUS_YEAR) + "M" + str(MONTH_TABLE_TO_TRUNCATE)
                logger.debug("Table to truncate: {}".format(TABLE_NAME))
                return TABLE_NAME

            if int(CURRENT_MONTH) == 3:
                PREVIOUS_YEAR = int(CURRENT_YEAR) - 1
                logger.debug("Previous YEAR: {}".format(PREVIOUS_YEAR))
                MONTH_TABLE_TO_TRUNCATE = MONTHS[-1:-3:-1][-1]
                logger.debug("Month Table to truncate: {}".format(MONTH_TABLE_TO_TRUNCATE))
                TABLE_NAME= "DEPLOYED_MESSAGES_V2_" + str(PREVIOUS_YEAR) + "M" + str(MONTH_TABLE_TO_TRUNCATE)
                logger.debug("Table to truncate: {}".format(TABLE_NAME))
                return TABLE_NAME

            if int(CURRENT_MONTH) == 4:
                PREVIOUS_YEAR = int(CURRENT_YEAR) - 1
                logger.debug("Previous YEAR: {}".format(PREVIOUS_YEAR))
                MONTH_TABLE_TO_TRUNCATE = MONTHS[-1:-2:-1][-1]
                logger.debug("Month Table to truncate: {}".format(MONTH_TABLE_TO_TRUNCATE))
                TABLE_NAME= "DEPLOYED_MESSAGES_V2_" + str(PREVIOUS_YEAR) + "M" + str(MONTH_TABLE_TO_TRUNCATE)
                logger.debug("Table to truncate: {}".format(TABLE_NAME))
                return TABLE_NAME
        else:
            MONTH_TABLE_TO_TRUNCATE = MONTHS[int(CURRENT_MONTH)-5:int(CURRENT_MONTH)-1]
            logger.debug(MONTH_TABLE_TO_TRUNCATE[0])
            logger.debug("Month Table to truncate: {}".format(MONTH_TABLE_TO_TRUNCATE[0]))
            TABLE_NAME = "DEPLOYED_MESSAGES_V2_" + str(CURRENT_YEAR) + "M" + str(MONTH_TABLE_TO_TRUNCATE[0])
            logger.debug("Table to truncate: {}".format(TABLE_NAME))
            return TABLE_NAME



def main():
    logger.debug("Inside main()")
    table_to_truncate=getArchiveTableName()
    logger.debug("Feed table to truncate to cqlsh: {}".format(table_to_truncate))
    truncateTable(table_to_truncate)


if __name__ == "__main__":
    main()
