import psycopg2
import os
import boto3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class PGConnection:
    def __init__(self, username, password, host, dbname):
        """ param username: username with access to a Postgres DB
             param password: user password with access to a Postgres DB
             param host: Hostname that is running Postgres DB
             param dbname: Name of database definied on Postgres DB
        """
        self.username = username
        self.password = password
        self.host = host
        self.dbname = dbname
    
    def DBConnect(self):
        try:
            self.connection = psycopg2.connect(host=self.host, dbname=self.dbname,user=self.username, password=self.password )
            return True
        except Exception as e:
            return e

    def DBWrite(self, file_name, file_status):
        """ param file_name: File name that will be stored on DB
            param file_status: status == False then its not synced with AWS S3.
        """
        cur = self.connection.cursor()
        cur.execute(f"insert into xml_controller(file_name, file_status) values('{file_name}',{file_status})")
        self.connection.commit()
    
    def DBRead(self, file_name):
        """ param file_name: File name that will be looked on DB
            Return: return a single record from given file name.
        """
        cur = self.connection.cursor()
        cur.execute(f"select file_name, file_status from xml_controller where file_name = '{file_name}'")
        return cur.fetchone()
    
    def DBUpdate(self, file_name, file_status):
        """ param file_name: File name that will be updated on DB
            param file_status: updated status with True or False, indicating that filed is stored or not on S3.
        """
        cur = self.connection.cursor()
        cur.execute(f"update xml_controller set file_status = {file_status} where file_name = '{file_name}'")
        self.connection.commit()
    
    def DBClose(self):
        """ Close connection with Database.
        """
        self.connection.close()

class S3Sync:
    def __init__(self, aws_access_key, aws_secret_access_key, bucket_name):
        """ param aws_access_key: Access key with access to S3 Bucket.
            param aws_secret_access_key: Secret key with access to S3 Bucket.
            param bucket_name: Bucket name that was created on S3
        """
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name = bucket_name

    def SendToS3(self, file_name, file_name_s3):
        """ param file_name: file name of file.
            param file_name_s3: file name that want to be stored on S3.
            Return: Return True in case of success upload or Return error message.
        """
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        s3 = session.resource('s3')
        # Filename - File to upload
        # Bucket - Bucket to upload to (the top level directory under AWS S3)
        # Key - S3 object name (can contain subdirectories). If not specified then file_name is used
        try:
            s3.meta.client.upload_file(file_name, self.bucket_name, file_name_s3)
            return True
        except Exception as e:
            return e

class AlertErrorEmail:
    def __init__(self, email_source, email_dest, email_server,email_subject, password):
        """ param email_source: email address that will send email.
            param emaiL_dest: email address that will receive email.
            param email_server: server address of smtp server.
            param email_subject: Subject of email that will be send.
            param password: password of email address that will send email.
        """
        self.email_source = email_source
        self.email_dest = email_dest
        self.password = password
        self.email_server = email_server
        self.email_subject = email_subject
    
    def CreateErrorList(self,error_message):
        msg = MIMEMultipart()
        msg['From'] = self.email_source
        msg['To'] = self.email_dest
        msg['Subject'] = self.email_subject
        message = error_message
        msg.attach(MIMEText(message))

        mailserver = smtplib.SMTP_SSL(self.email_server,465)
        # identify ourselves to smtp gmail client
        # secure our email with tls encryption
        # re-identify ourselves as an encrypted connection
        mailserver.ehlo()
        mailserver.login(self.email_source, self.password)

        mailserver.sendmail(self.email_source,self.email_dest,msg.as_string())

        mailserver.quit()
if __name__ == "__main__":
    #connection to DB details
    user_dblocal = ''
    password_dblocal = ''
    address_dblocal = ''
    database = ''

    #Connect to db and see if file not sync
    start = PGConnection(user_dblocal,password_dblocal,address_dblocal,database)
    conecta_db = start.DBConnect()

    #Email connection details
    email_source = ""
    email_dest = ""
    emaiL_server = ""
    email_subject = ""
    emaiL_password = ""
    email_alert = AlertErrorEmail(email_source, email_dest,emaiL_server,email_subject,emaiL_password)

    #OS Path of folder that will be synced with S3
    os_path = "/"
    if conecta_db == True:
        try:
            files = os.listdir(os_path)
        except Exception as e:
                print("problem with directory")
                email_alert.CreateErrorList("Message that will be send in case of directory is not available")

        #Credencials for access S3 Bucket
        s3_access_key = ""
        s3_shared_secret = ""
        bucket_name = ""
        #iterate over list of xmls in dir
        for file_name in files:
            #if xml not in DB, send file to S3 then update database:
            check_db = start.DBRead(file_name)
            
            #check if have xml file name in DB, if its null then dont have that record in db
            if check_db == None:
                s3connection = S3Sync(s3_access_key, s3_shared_secret, bucket_name)
                upload_file = s3connection.SendToS3(f"{os_path}/{file_name}", file_name)
                #If upload to S3 doenst have any error, than return as True
                if upload_file == True:
                    insert_db = start.DBWrite(file_name, True)
                #But if have error during Sync with S3 than insert on DB False
                else:
                    insert_db = start.DBWrite(file_name, False)
                    email_alert.CreateErrorList("Message that will be send in case of S3 bucket is not available")


            else:
                #If a file is marked as False in DB try Sync with S3 again
                if check_db[1] == False:
                    s3connection = S3Sync(s3_access_key, s3_shared_secret, bucket_name)
                    upload_file = s3connection.SendToS3(f"{os_path}/{file_name}")
                    if upload_file == True:
                        update_db = start.DBUpdate(file_name, True)
                    else:
                        email_alert.CreateErrorList("Message that will be send in case of S3 bucket is not available")
                else:
                    continue
        close_db = start.DBClose()

    else:
        email_alert.CreateErrorList("Message that will be send in case of DB Server is not available")
