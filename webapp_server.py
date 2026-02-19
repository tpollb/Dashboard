import os
import logging
from flask import Flask, render_template, request
from dotenv import load_dotenv

load_dotenv('.env.local')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/chart')
def chart_page():
    metric = request.args.get('metric', 'temperature')
    period = request.args.get('period', '24h')
    logger.info(f"📊 Запрос графика: metric={metric}, period={period}")
    return render_template('chart.html', metric=metric, period=period)

@app.route('/health')
def health():
    return {"status": "ok", "version": "v6.0"}

if __name__ == '__main__':
    port = int(os.getenv('WEBAPP_PORT', 5000))
    logger.info(f"🚀 Web App сервер запущен на порту {port}")
    logger.info(f"📎 URL для тестов: http://localhost:{port}/chart?metric=temperature&period=24h")
    app.run(host='0.0.0.0', port=port, debug=False)