from logging.handlers import BufferingHandler
import smtplib


class BufferingSMTPHandler(BufferingHandler):
    def __init__(self, user, password,
                 email_to, email_subject, email_from=None,
                 capacity=10000, host='smtp.gmail.com', port=465):
        BufferingHandler.__init__(self, capacity)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.email_from = user if email_from is None else email_from
        self.email_to = email_to
        self.email_subject = email_subject

    def flush(self):
        if len(self.buffer) > 0:
            self.acquire()
            try:
                smtp_server = smtplib.SMTP_SSL(self.host, self.port)
                smtp_server.login(self.user, self.password)

                email_body = '\n'.join(self.format(r) for r in self.buffer)
                if isinstance(self.email_to, str):
                    email_to = self.email_to
                else:
                    email_to = ','.join(self.email_to)

                email_tmpl = ("From: {from}\nTo: {to}\nSubject: {subject}"
                              "\n\n{body}")
                email_args = {'from': self.email_from,
                              'to': email_to,
                              'subject': self.email_subject,
                              'body': email_body
                             }
                email_text = email_tmpl.format(**email_args)

                smtp_server.sendmail(self.email_from, self.email_to, email_text)

                self.buffer = []
            finally:
                smtp_server.close()
                self.release()
