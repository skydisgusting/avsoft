import mysql.connector
import json
import requests
from pydantic import BaseModel
from typing import List


# Конфигурационный класс
class Config(BaseModel):
    mysql_host: str
    mysql_user: str
    mysql_password: str
    mysql_database: str
    wise_reader_host: str
    threshold: int
    telegram_token: str
    telegram_recipient_id: str


# Модель данных для уведомлений
class Notification(BaseModel):
    word: str
    count: int
    file_paths: List[str]


def load_config():
    with open('config.json', 'r') as f:
        config_data = json.load(f)

        return Config(**config_data)


def send_notification(notification: Notification):
    message = f'Слово "{notification.word}" появилось {notification.count} раз'
    message += ' Пути до файлов: \n\n {}'.format(", \n\n".join(notification.file_paths))

    token = config.telegram_token
    chat_id = config.telegram_recipient_id

    response = requests.post(
        url='https://api.telegram.org/bot{0}/sendMessage'.format(token),
        data={'chat_id': chat_id, 'text': message}
    ).json()

    if response['ok'] is True:
        return True


def process_word_counts(config: Config):
    connection = mysql.connector.connect(
        host=config.mysql_host,
        user=config.mysql_user,
        password=config.mysql_password,
        database=config.mysql_database
    )
    cursor = connection.cursor()

    cursor.execute(f"SELECT word, file_paths, count FROM words WHERE count >= {config.threshold}")
    results = cursor.fetchall()
    for result in results:
        word, file_paths, count = result
        file_paths = file_paths.split(',')
        notification = Notification(word=word, count=count, file_paths=file_paths)
        send_notification(notification)

        cursor.execute("UPDATE words SET count=0, file_paths='' WHERE word='{}'".format(word))

    cursor.close()
    connection.commit()
    connection.close()


if __name__ == '__main__':
    config = load_config()
    process_word_counts(config)
