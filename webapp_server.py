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
    """API endpoint для получения РЕАЛЬНЫХ данных из БД"""
    tag_id = request.args.get('metric', '1')
    period = request.args.get('period', '24h')
    
    try:
        tag_id = int(tag_id)
        db = get_db()
        
        # ✅ Запрашиваем реальные данные с динамическим лимитом
        rows = run_async(db.get_metrics(tag_id, period, limit=None))
        
        if not rows:
            return jsonify({
                "labels": [],
                "values": [],
                "unit": "",
                "count": 0,
                "message": "Нет данных за выбранный период",
                "period_requested": period
            })
        
        # ✅ Форматируем метки времени в зависимости от периода
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
        
        # ✅ Проверяем реальный диапазон данных
        first_ts = rows[0][0]
        last_ts = rows[-1][0]
        
        unit = run_async(db.get_metric_unit(tag_id))
        
        logger.info(f"✅ Возвращено {len(labels)} точек для tag_id={tag_id}, period={period}")
        logger.info(f"   📅 Диапазон: {first_ts} → {last_ts}")
        
        return jsonify({
            "labels": labels,
            "values": values,
            "unit": unit,
            "count": len(rows),
            "period": period,
            "tag_id": tag_id,
            "first_timestamp": first_ts.isoformat(),
            "last_timestamp": last_ts.isoformat()
        })
        
    except ValueError:
        logger.error(f"❌ Неверный tag_id: {request.args.get('metric')}")
        return jsonify({"error": "Invalid tag_id"}), 400
    except Exception as e:
        logger.error(f"❌ Ошибка API /api/ {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/tags')
def get_tags():
    """API endpoint для получения списка тегов"""
    try:
        db = get_db()
        tags = run_async(db.get_available_tags(limit=50, with_data_only=True))
        return jsonify({"tags": [{"id": t[0], "name": t[1], "count": t[2]} for t in tags]})
    except Exception as e:
        logger.error(f"❌ Ошибка API /api/tags: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health')
def health():
    return jsonify({"status": "ok", "version": "v9.0", "db": "postgres"})

if __name__ == '__main__':
    port = int(os.getenv('WEBAPP_PORT', 10000))
    logger.info(f"🚀 Web App сервер запущен на порту {port}")
    logger.info(f"🗄️ PostgreSQL: {os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}")
    app.run(host='0.0.0.0', port=port, debug=False)