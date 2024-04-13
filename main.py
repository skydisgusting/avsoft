import json
import pika


def send_test_message(queue_name, file_path):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps({'file_path': file_path}))
    connection.close()


# Отправка сообщения с текстовым файлом
send_test_message('Search', 'test.txt')
send_test_message('Search', 'test2.txt')


# CREATE TABLE `avsoft`.`words` (
#   `word` VARCHAR(50) NULL,
#   `file_paths` MEDIUMTEXT NOT NULL,
#   `count` INT NULL DEFAULT 1,
#   UNIQUE INDEX `word_UNIQUE` (`word` ASC));


