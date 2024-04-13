from flask import Flask, jsonify
from wise_reader import load_config
import threading
import requests

app = Flask(__name__)
config = load_config()
wise_reader_host = config.wise_reader_host  # Имя хоста WiseReader в сети Docker


@app.route('/stats', methods=['GET'])
def get_stats():
    # Запрос статистики из WiseReader
    try:
        response = requests.get(f'http://{wise_reader_host}:5000/stats')
        if response.status_code == 200:
            stats = response.json()
            return jsonify(stats)
        else:
            return jsonify({'error': 'Failed to fetch stats from WiseReader'}), 500
    except Exception as e:
        return jsonify({'error': f'Error fetching stats: {str(e)}'}), 500


if __name__ == '__main__':
    # Запуск HTTP-сервера в отдельном потоке
    server_thread = threading.Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': 5001})
    server_thread.daemon = True
    server_thread.start()
