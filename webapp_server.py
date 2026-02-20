import os
import logging
import asyncio
from flask import Flask, render_template, request, jsonify, redirect
from dotenv import load_dotenv
from db_connector import get_db, run_async

load_dotenv('.env.local')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ✅ Разрешаем CORS для ngrok и Telegram
@app.after_request
def add_header(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response

logger.info("🚀 Инициализация Flask Web App...")

@app.route('/')
def root_redirect():
    return redirect('/chart?metric=1&period=24h', code=302)

@app.route('/chart')
def chart_page():
    metric = request.args.get('metric', '1')
    period = request.args.get('period', '24h')
    logger.info(f"📊 Запрос графика: tag_id={metric}, period={period}")
    return render_template('chart.html', metric=metric, period=period)

@app.route('/api/data')
def get_chart_data():
    tag_id = request.args.get('metric', '1')
    period = request.args.get('period', '24h')
    
    try:
        tag_id = int(tag_id)
        db = get_db()
        rows = run_async(db.get_metrics(tag_id, period, limit=None))
        
        if not rows:
            return jsonify({
                "labels": [], "values": [], "unit": "", "count": 0,
                "message": "Нет данных за выбранный период"
            })
        
        labels = []
        values = []
        
        for ts, val in rows:
            if period == '1h':
                labels.append(ts.strftime("%H:%M"))
            elif period == '24h':
                labels.append(ts.strftime("%d.%m %H:%M"))
            elif period == '7d':
                labels.append(ts.strftime("%d.%m %H:%M"))
            else:
                labels.append(ts.strftime("%d.%m %H:%M"))
            values.append(round(val, 2) if val is not None else None)
        
        unit = run_async(db.get_metric_unit(tag_id))
        
        return jsonify({
            "labels": labels, "values": values, "unit": unit,
            "count": len(rows), "period": period, "tag_id": tag_id
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка API: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/tags')
def get_tags():
    try:
        db = get_db()
        tags = run_async(db.get_available_tags(limit=50, with_data_only=True))
        return jsonify({"tags": [{"id": t[0], "name": t[1], "count": t[2]} for t in tags]})
    except Exception as e:
        logger.error(f"❌ Ошибка /api/tags: {e}")
        return jsonify({"error": str(e), "tags": []}), 500

@app.route('/health')
def health():
    return jsonify({"status": "ok", "version": "v9.0", "db": "postgres"})

if __name__ == '__main__':
    port = int(os.getenv('WEBAPP_PORT', 10000))
    logger.info(f"🚀 Web App сервер запущен на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False)