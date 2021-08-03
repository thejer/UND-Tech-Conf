import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    host = "techconfdb.postgres.database.azure.com"
    dbname = "techconfdb"
    user = "techconfadmin@techconfdb"
    password = 'Pa$$w0rd1234'
    sslmode = "require"
    connection_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    connection = psycopg2.connect(connection_string)

    try:
        cursor = connection.cursor()

        # TODO: Get notification message and subject from database using the notification_id
        cursor.execute("SELECT * FORM notification WHERE id = %s;", notification_id)
        notification = cursor.fetchall

        # TODO: Get attendees email and name
        cursor.execute("SELECT * FROM attendee;")
        attendees = cursor.fetchall

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            subject = '{}: {}'.format(attendee.first_name, notification.subject)
            message = Mail(
                from_email='info@techconf.com',
                to_email=attendee.email,
                subject=subject
            )
            # Send email with Send Grid API
            
            # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
            notification_status = 'Notified {} attendees'.format(len(attendees))
            cursor.execute("""UPDATE notification
                            SET status=%s,
                            completed_date=%s,
                            submitted_date=%s,
                            WHERE id = %s;""", 
                            (notification_status, datetime.utcnow(), datetime.utcnow(), notification_id,))
            connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        logging.info("closing db connection")
        cursor.close()
        connection.close()