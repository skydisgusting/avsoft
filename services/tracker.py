import os
import json
import pika
import shutil
from pydantic import BaseModel

os.chdir('C:/Users/joe/pp/test-assignment/avsoft-test-assignment')


class Config(BaseModel):
    rabbitmq_host: str
    parsing_queue_name: str
    errors_queue_name: str
    analyzer_volume_path: str
    error_volume_path: str


def load_config():
    with open('services/config.json', 'r') as f:
        config_data = json.load(f)

        return Config(**config_data)


def callback(ch, method, properties, body):
    json_data = json.loads(body.decode('utf-8'))
    file_path = json_data.get('file_path', '')

    if file_path.endswith('.txt'):
        move_file_to_analyzer(file_path)
        send_message_to_parsing_queue(file_path)
    else:
        move_file_to_error_handler(file_path)
        send_message_to_errors_queue(file_path)


def move_file_to_analyzer(file_path):
    new_file_path = os.path.join(config.analyzer_volume_path, os.path.basename(file_path))
    shutil.move(file_path, new_file_path.replace("\\", "/"))


def move_file_to_error_handler(file_path):
    new_file_path = os.path.join(config.error_volume_path, os.path.basename(file_path))
    shutil.move(file_path, new_file_path.replace("\\", "/"))


def send_message_to_parsing_queue(file_path):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.rabbitmq_host))
    channel = connection.channel()
    channel.queue_declare(queue=config.parsing_queue_name)
    channel.basic_publish(exchange='',
                          routing_key=config.parsing_queue_name,
                          body=json.dumps({'file_path': file_path}))

    connection.close()


def send_message_to_errors_queue(file_path):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.rabbitmq_host))
    channel = connection.channel()
    channel.queue_declare(queue=config.errors_queue_name)
    channel.basic_publish(exchange='',
                          routing_key=config.errors_queue_name,
                          body=json.dumps({'file_path': file_path}))

    connection.close()


def start_searcher():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.rabbitmq_host))
    channel = connection.channel()
    channel.queue_declare(queue='Search')
    channel.basic_consume(queue='Search', on_message_callback=callback, auto_ack=True)
    print('[TRACKER HANDLER] status up..')
    channel.start_consuming()


if __name__ == '__main__':
    config = load_config()
    start_searcher()
