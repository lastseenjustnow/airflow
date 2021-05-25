import imaplib
import os
import email
import re
import logging


def download_attachments(
        output_folder: str,
        sender: str,
        date_from: str,
        date_to: str):

    """
    This function allows to collect .csv files from the attachments of given inbox
    and put them all to a predefined folder.

    :param output_folder: destination folder for attachments
    :param sender: IMAP filter by sender, equivalent of "FROM" statement
    :param date_from: IMAP filter by date, equivalent of "SINCE" statement
    :param date_to: IMAP filter by date, equivalent of "BEFORE" statement
    :return: status

    For info see: https://datatracker.ietf.org/doc/html/rfc3501
    """

    imap_filter = '(FROM "{}" SINCE "{}" BEFORE "{}")' \
        .format(sender, date_from, date_to)

    email_user = ''
    email_pass = ''
    email_host = 'smtp.office365.com'
    port = 993

    mail = imaplib.IMAP4_SSL(email_host,port)
    mail.login(email_user, email_pass)
    mail.select()

    type, data = mail.search(None, imap_filter)
    mail_ids = data[0]
    id_list = mail_ids.split()
    print(id_list)

    logging.info("Checking letters using the following IMAP condition: " + imap_filter)

    for num in data[0].split():
        typ, data = mail.fetch(num, '(RFC822)')
        raw_email = data[0][1]

        raw_email_string = raw_email.decode('utf-8')
        email_message = email.message_from_string(raw_email_string)

        for part in email_message.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            fileName = part.get_filename()
            if not bool(re.search('.*\.csv', fileName)):
                continue

            if bool(fileName):
                filePath = os.path.join(output_folder, fileName)
                if not os.path.isfile(filePath):
                    fp = open(filePath, 'wb')
                    fp.write(part.get_payload(decode=True))
                    fp.close()

            logging.info("File has been downloaded: {}.".format(fileName))

    return "Files downloaded."