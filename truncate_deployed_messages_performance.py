#!/usr/bin/env python

from datetime import date

import datetime
import subprocess
import smtplib
import logging
import time
import socket

TARGET_ENV="ENV_NAME"
KEYSPACE="KEYSPACE_NAME"
#time to wait to run cleanup
TIME_TO_WAIT=10
# Flag to enable/disable email notification
sendEmailNotification = True
SMTP_HOST='SMTP HOST'


cqlsh_path='/opt/app/cassandra/bin/cqlsh'
cassandra_nodes='/opt/app/truncate_cron/cassandra_nodes.txt'
nodetool_path='/opt/app/cassandra/bin/nodetool'
truncate_cql='/opt/app/truncate_cron/truncate.cql'
log_file='/opt/app/truncate_cron/truncate_cron.log'



# Email sender
sender = 'sender@email.com'

# List of email receivers
receivers = ['receiver@email.com']


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
    # logging.debug('Subject: ', SUBJECT)

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
        logging.debug('Successfully sent email')
    except:
        logging.debug('sendEmailNotification Error: unable to send email')
    finally:
        smtpObj.quit()




def isLeapYear(YEAR):
    if (YEAR % 4) == 0:
        if (YEAR % 100) == 0:
            if (YEAR % 400) == 0:
                return True
            else:
                return False
        else:
            return True
    else:
        return False


def getTableToTruncate():
    weekDays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

    today = date.today()

    logging.debug("Today is {}".format(today))
    # logging.debug()
    CURRENT_YEAR = today.strftime("%G")
    CURRENT_WEEK = today.strftime("%V")

    logging.debug("Type of CURRENT_WEEK: {}".format(type(CURRENT_WEEK)))
    # Testing with hardcoded YEAR
    # CURRENT_YEAR = 2021
    # CURRENT_WEEK = "11"

    # logging.debug("ISO CURRENT_YEAR: {}".format(CURRENT_YEAR))
    # logging.debug("ISO CURRENT_WEEK: {}".format(CURRENT_WEEK))
    # logging.debug("Int WEEK NUMBER: {}".format(int(CURRENT_WEEK)))
    logging.debug()

    if CURRENT_WEEK == "01":
        logging.debug("Logic to handle truncating previous years' last week table comes here ")

        PREVIOUS_YEAR = int(CURRENT_YEAR) - 1
        logging.debug("Previous YEAR: {}".format(PREVIOUS_YEAR))

        # Determine if previous year has 52 or 53 weeks
        first_day_of_previous_year = datetime.datetime(PREVIOUS_YEAR, 1, 1).weekday()

        logging.debug("First day of previous year is {}".format(weekDays[first_day_of_previous_year]))
        # if the year starts on a Thursday or is a leap year that starts on a Wednesday, that particular year will have 53 numbered weeks.

        if first_day_of_previous_year == 3 or (isLeapYear(PREVIOUS_YEAR) and first_day_of_previous_year == 2):
            LAST_WEEK_NUMBER = 53
            logging.debug("Last week number of previous year {} is {}".format(PREVIOUS_YEAR,LAST_WEEK_NUMBER))
            # logging.debug("DEPLOYED_MESSAGES_V2_" + str(PREVIOUS_YEAR) + "W" + str(LAST_WEEK_NUMBER))
            return "DEPLOYED_MESSAGES_V2" + str(PREVIOUS_YEAR) + "W" + str(LAST_WEEK_NUMBER)
        else:
            logging.debug("Previous year: {} -> First day was {}".format(PREVIOUS_YEAR, "MONDAY"))
            LAST_WEEK_NUMBER = 52
            logging.debug("Last week number of previous year {} is {}".format(PREVIOUS_YEAR, LAST_WEEK_NUMBER))
            # logging.debug("DEPLOYED_MESSAGES_V2_" + str(PREVIOUS_YEAR) + "W" + str(LAST_WEEK_NUMBER))
        return "DEPLOYED_MESSAGES_V2_" + str(PREVIOUS_YEAR) + "W" + str(LAST_WEEK_NUMBER)
    else:
        LAST_WEEK_NUMBER = int(CURRENT_WEEK) - 1
        # Prepend 0 for Weeks less than 10
        if LAST_WEEK_NUMBER < 10:
            LAST_WEEK_NUMBER = "0" + str(LAST_WEEK_NUMBER)
        return "DEPLOYED_MESSAGES_V2_" + str(CURRENT_YEAR) + "W" + str(LAST_WEEK_NUMBER)


def truncateTable(table_name):
    logging.debug("Inside truncateTable()")
    logging.debug("Table to truncate: {}".format(table_name))
    CQL_STATEMENT='TRUNCATE ' + KEYSPACE + '."' + table_name + '";'
    logging.debug("CQL_STATEMENT: {}".format(CQL_STATEMENT))

    with open(truncate_cql, 'w') as file:
        file.write(CQL_STATEMENT)

    try:
        call_list = []
        call_list.append(cqlsh_path)
        call_list.append(socket.gethostname())
        call_list.append('-f')
        call_list.append(truncate_cql)
        logger.debug("Running CQL Truncate:")
        subprocess.call(call_list)
    except:
        event_subject = "Error running cql script on via cron to truncate " + table_name + " on " + TARGET_ENV
        event_body = "Unable run cql script via cron to truncate " + table_name + "Please check logs"
        sendEmailNotification(event_subject, event_body)
    else:
        # Send Email Notification
        event_subject = "Truncated " + table_name +  " on Cassandra Performance Cluster - " + TARGET_ENV
        event_body = event_subject
        if sendEmailNotification:
            sendEmailNotification(event_subject, event_body)
    finally:
        logging.debug("Pausing for {} seconds".format(TIME_TO_WAIT))
        time.sleep(TIME_TO_WAIT)
        run_cleanup()

def run_cleanup():
    with open('cassandra_nodes.txt', 'r') as file:
         servers=file.readlines()
         call_list = []
         for server in servers:
            #  logging.debug("Cleanup running on {}".format(server.strip("\n")))
             call_list.append(nodetool_path)
             call_list.append("-h")
             call_list.append(server.strip("\n"))
             call_list.append("-p")
             call_list.append("7199")
             call_list.append("clearsnapshot")
             logger.debug("Running nodetool cleanup on {}".format(server.strip("\n")))      
             subprocess.call(call_list)
             del call_list[:]

def main():
    logging.debug()
    logging.debug("Inside main()")
    table_name=getTableToTruncate()

    # Call truncateTable here
    truncateTable(table_name)

if __name__ == "__main__":
    main()
