import urllib
import sqlalchemy
import pyodbc
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


class MicrosoftServer:
    def __init__(self, server, username, password):
        self.server = server
        self.username = username
        self.password = password
        self.driver = '{ODBC Driver 17 for SQL Server}'


def getCursor(ms: MicrosoftServer, db):
    params = 'DRIVER=' + ms.driver + ';SERVER=' + ms.server + \
             ';PORT=1433;DATABASE=' + db + ';UID=' + ms.username + ';PWD=' + ms.password
    return pyodbc.connect(params, autocommit=True).cursor()


def getEngine(ms: MicrosoftServer, db):
    params = 'DRIVER=' + ms.driver + ';SERVER=' + ms.server + \
        ';PORT=1433;DATABASE=' + db + ';UID=' + ms.username + ';PWD=' + ms.password
    db_params = urllib.parse.quote_plus(params)
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={}".format(db_params), pool_pre_ping=True)

    # TODO: research why this caused 'Unicode conversion failed' (0)
    #@event.listens_for(engine, "before_cursor_execute")
    #def receive_before_cursor_execute(
    #        conn, cursor, statement, params, context, executemany
    #):
    #    if executemany:
    #        cursor.fast_executemany = True
    return engine

def getConn(ms: MicrosoftServer, db):
    params = 'DRIVER=' + ms.driver + ';SERVER=' + ms.server + \
             ';PORT=1433;DATABASE=' + db + ';UID=' + ms.username + ';PWD=' + ms.password
    return pyodbc.connect(params, autocommit=True)

def sendMail(send_from, send_to, subject, text, files=None,
              server="smtp.office365.com"):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    smtp = smtplib.SMTP(server, 587)
    smtp.connect(server, 587)
    smtp.starttls()
    smtp.login(send_from, "")
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

vlad_201 = MicrosoftServer('192.168.1.201', 'vlad', '')

# Engines
database_js = 'Jsoham'
engine_js = getEngine(vlad_201, database_js)

database_frx = 'JsohamFRX'
engine_frx = getEngine(vlad_201, database_frx)

database_aarna = 'AarnaProcess'
engine_aarna = getEngine(vlad_201, database_aarna)
