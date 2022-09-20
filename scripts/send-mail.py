# using SendGrid's Python Library
# https://github.com/sendgrid/sendgrid-python
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

# setting up api key
# use your own api key here
# load your environment
load_dotenv()
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')

message = Mail(
    from_email='fisseha.137@gmail.com',
    to_emails='rasfish5@gmail.com',
    subject='Sending with SendGrid is Fun',
    html_content='<strong>and easy to do anywhere, even with Python</strong>')
try:
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    print(f"response status code: {response.status_code}")
    print(f"response body: {response.body}")
    print(f"response header: {response.headers}")
except Exception as e:
    print(e.message)
