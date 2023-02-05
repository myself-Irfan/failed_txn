# DB adapter which implements of Python DB API
import psycopg2
# package to view at console
# from prettytable import PrettyTable
# table = PrettyTable()
# package to use for data analysis
import pandas as pd
# package to send pretty html table
from pretty_html_table import build_table
# defines an SMTP client session object for use in sending emails
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText 

def create_connection():
    # establishing the connection
    conn = psycopg2.connect(
        database="TallyPlus",
        user='postgres',
        password='1234',
        host='localhost',
        port= '5432')
    # return connection obj
    return conn

def rocket_table(conn):
    # hard-coding query to a var
    query = '''select created_at, updated_at, card_number, rrn, amount, status, remarks, txn_id, txn_ref_num, description
            from rocket_txn
            where status not in ('FAILED', 'SUCCESS')
            --and created_at >= (NOW() - INTERVAL '1 hour')
            order by created_at desc;'''
    
    # used to execute statements to communicate with database
    cur = conn.cursor()
    cur.execute(query)
    
    # saving to a tuple
    rows = cur.fetchall()
    
    # constructing dataframe from a tuple
    data_frame = pd.DataFrame(data=rows)
    data_frame.columns = [
        'created_at',
        'updated_at',
        'card_number',
        'rrn',
        'amount',
        'status',
        'remarks',
        'txn_id',
        'txn_ref_num',
        'description']
    
    rocket_t = build_table(data_frame, 'grey_dark')
    
    return rocket_t
    
def nagad_table(conn):
    # hard-coding query to a var
    query = '''select created_at, updated_at, order_id, amount, status, remark, additional_merchant_info 
            from nagad_txn
            where status not in ('FAILED', 'SUCCESS', 'REFUNDED')
            --and created_at >= (NOW() - INTERVAL '1 hour')
            order by created_at desc;
            '''
    
    # used to execute statements to communicate with database
    cur = conn.cursor()
    cur.execute(query)
    
    # saving to a tuple
    rows = cur.fetchall()
    
    # constructing dataframe from a tuple
    data_frame = pd.DataFrame(data=rows)
    data_frame.columns = [
        'created_at',
        'updated_at',
        'order_id',
        'amount',
        'status',
        'remark',
        'additional_merchant_info']
    
    nagad_t = build_table(data_frame, 'grey_dark')
    
    return nagad_t
    

def send_mail(HTML, r_add):
    #The mail addresses and password
    sender_address = 'irfan.ahmed@tallykhata.com'
    sender_pass = 'zizobnxxerpzzjsk'
    receiver_address = r_add
    #'mahidul24@gmail.com'
    
    #Setup the MIME
    message = MIMEMultipart("alternative", None, [MIMEText(HTML, 'html')])
    message['From'] = 'irfan.ahmed@tallykhata.com'
    message['To'] = receiver_address
    message['Subject'] = 'Rocket & Nagad Transactions'
    
    #message.attach(MIMEText(msg, 'plain'))
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    msg = message.as_string()
    session.sendmail(sender_address, receiver_address, msg)
    session.quit()
    print('Mail Sent')


def main():
    r_add = ['overlordahmed.irfan@gmail.com', 'ahmed.1995.irfan@gmail.com']
    conn = create_connection()
    
    with conn:
        print("Running query")
        html = '''<html><head></head><body>Rocket Transactions''' + rocket_table(conn) + "\nNagad Transactions\n" + nagad_table(conn) + '''</body></html>'''

    for _ in r_add:
        #send_mail(html, _)
    
main()

