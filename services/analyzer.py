import pika
import mysql.connector
import os
import json
from pydantic import BaseModel

os.chdir('C:/Users/joe/pp/test-assignment/avsoft-test-assignment')


class Config(BaseModel):
    rabbitmq_host: str
    analyzer_volume_path: str
    parsing_queue_name: str
    mysql_host: str
    mysql_user: str
    mysql_password: str
    mysql_database: str


def load_config():
    with open('services/config.json', 'r') as f:
        config_data = json.load(f)

        return Config(**config_data)


def callback(ch, method, properties, body):
    json_data = json.loads(body.decode('utf-8'))
    file_path = json_data.get('file_path', '')
    words = extract_words_from_file(file_path)
    save_words_to_database(words, config.analyzer_volume_path + file_path)


def extract_words_from_file(file_path):
    with open(config.analyzer_volume_path + file_path, 'r', encoding="utf-8") as file:
        text = file.read()
        words = [word.strip(".,!?;:\"'") for word in text.split()]

        return words


def save_words_to_database(words, file_path):
    connection = mysql.connector.connect(host=config.mysql_host, user=config.mysql_user, password=config.mysql_password,
                                         database=config.mysql_database)

    cursor = connection.cursor()
    for word in words:

        # Проверка при конкатенации, чтобы в бд путь файла записывался один раз
        # даже если слово встречалось несколько раз

        cursor.execute("""
            INSERT INTO words (word, file_paths, count)
            VALUES (%s, %s, 1)
            ON DUPLICATE KEY UPDATE
            file_paths = IF(FIND_IN_SET(%s, file_paths), file_paths, CONCAT(file_paths, ',', %s)),
            count = count + 1
        """, (word, file_path, file_path, file_path))

    connection.commit()
    cursor.close()
    connection.close()


def start_analyzer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.rabbitmq_host))
    channel = connection.channel()
    channel.queue_declare(queue=config.parsing_queue_name)
    channel.basic_consume(queue=config.parsing_queue_name, on_message_callback=callback, auto_ack=True)
    print('[ANALYZER HANDLER] status up..')
    channel.start_consuming()


if __name__ == '__main__':
    config = load_config()
    start_analyzer()
