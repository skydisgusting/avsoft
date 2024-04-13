import pika
import smtplib
import json
import requests
from email.mime.text import MIMEText
from pydantic import BaseModel


class Config(BaseModel):
    rabbitmq_host: str
    errors_queue_name: str
    telegram_token: str
    telegram_recipient_id: int


def load_config():
    with open('config.json', 'r') as f:
        config_data = json.load(f)

        return Config(**config_data)


def callback(ch, method, properties, body):
    json_data = json.loads(body.decode('utf-8'))
    file_path = json_data.get('file_path', '')

    send_notification(file_path)


def send_notification(file_path):
    message = f'[ERROR HANDLER] error processing {file_path}'
    token = config.telegram_token
    chat_id = config.telegram_recipient_id

    response = requests.post(
        url='https://api.telegram.org/bot{0}/sendMessage'.format(token),
        data={'chat_id': chat_id, 'text': message}
    ).json()

    if response['ok'] is True:
        return True

    # email = MIMEText(message)
    # email['From'] = config.sender_email
    # email['To'] = config.recipient_email
    # email['Subject'] = 'Error Notification'
    # smtp_server = smtplib.SMTP(config.smtp_host, config.smtp_port)
    # smtp_server.starttls()
    # smtp_server.login(config.smtp_username, config.smtp_password)
    # smtp_server.sendmail(config.sender_email, config.recipient_email, email.as_string())
    # smtp_server.quit()


def start_error_handler():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.rabbitmq_host))
    channel = connection.channel()
    channel.queue_declare(queue=config.errors_queue_name)
    channel.basic_consume(queue=config.errors_queue_name, on_message_callback=callback, auto_ack=True)
    print('[ERROR HANDLER] status up..')
    channel.start_consuming()


if __name__ == '__main__':
    config = load_config()
    start_error_handler()
