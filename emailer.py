import smtplib
from email.mime.text import MIMEText
from private import gmail_password
import logging
import datetime

elog = logging.getLogger('email_logger')
eformatter = logging.Formatter('%(asctime)s,%(msecs)03d...%(levelname)s:  %(message)s')
ehandler = logging.FileHandler('./logs/{}.email.log'.format(datetime.date.today()))
ehandler.setFormatter(eformatter)
elog.addHandler(ehandler)
elog.setLevel(logging.DEBUG)

elog.info('Email log initialized')

default_sw = '\n\n--------------------------------------------------------------\n'


class Emailer:
    def __init__(self, fromaddress='cawasa.scan@gmail.com',
                 toaddress='seansissman@gmail.com, petryszynjr@gmail.com',
                 subject='CAWASA Scan',
                 body=''):
        self.fromaddress = fromaddress
        self.toaddress = toaddress
        self.subject= subject
        self.body = body

    def append_body(self, text, startswith=default_sw):
        """ Appends new text to the email body. """
        if len(text) >= 20:
            elog.info('Appending the text that starts with "{}" '
                      'to the email body.'.format(text[:20]))
        else:
            elog.info('Appending "{}" to the email body.'.format(text))
        self.body = self.body + startswith + text

    def send_email(self):
        """ Sends the instance email. """
        msg = MIMEText(self.body)
        msg['From'] = self.fromaddress
        msg['To'] = self.toaddress
        msg['Subject'] = self.subject
        message = msg.as_string()
        s = None
        try:
            elog.info('Attempting to send email\nFrom:\t{}\nTo:\t{}'
                      '\nSubject:\t{}'.format(self.fromaddress, self.toaddress,
                                              self.subject))
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(self.fromaddress, gmail_password())
            s.sendmail(self.fromaddress, self.toaddress, message)
        except:
            elog.exception('Email Failed!')
            raise
        finally:
            if s:
                s.quit()
                elog.info('Email successfully sent.')
