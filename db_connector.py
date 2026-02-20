import os
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

try:
    import asyncpg
except ImportError:
    asyncpg = None
    logging.warning("⚠️ asyncpg не установлен: pip install asyncpg")

load_dotenv('.env.local')

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Коннектор к PostgreSQL на asyncpg"""
    
    def __init__(self):
        logger.info("🗄️ Инициализация DatabaseConnector (asyncpg)")
        
        self.host = os.getenv('DB_HOST', '172.17.130.54')
        self.port = int(os.getenv('DB_PORT', '5432'))
        self.database = os.getenv('DB_NAME', 'postgres')
        self.user = os.getenv('DB_USER', 'user')
        self.password = os.getenv('DB_PASSWORD', 'user1234')
        self.schema = os.getenv('DB_SCHEMA', 'public')
        
        self.table_metrics = os.getenv('DB_METRICS_TABLE', 'tags_value')
        self.col_timestamp = os.getenv('DB_TIMESTAMP_COLUMN', 'date_created')
        self.col_tag = os.getenv('DB_TAG_COLUMN', 'tag_id')
        self.col_value = os.getenv('DB_VALUE_COLUMN', 'value')
        
        self.table_tags = os.getenv('DB_TAGS_DICT_TABLE', 'tags_dict')
        self.col_tag_name = os.getenv('DB_TAG_NAME_COLUMN', 'tag_name')
        
        self._pool = None
        logger.info(f"   ✓ Host: {self.host}:{self.port}/{self.database}")
        logger.info(f"   ✓ Таблица: {self.table_metrics}")
    
    async def get_pool(self):
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=2,
                max_size=10,
                command_timeout=30
            )
            logger.info("   ✓ Пул подключений создан")
        return self._pool
    
    async def get_metrics(self, tag_id: int, period: str, limit: int = None):
        """
        Получает данные тега за период.
        ✅ ИСПРАВЛЕНО: Корректные имена колонок после подзапроса
        """
        logger.info(f"📊 Запрос: tag_id={tag_id}, period={period}")
        
        # Динамический расчет диапазона и лимита
        period_map = {
            '1h':  {'delta': timedelta(hours=1),   'limit': 120},
            '24h': {'delta': timedelta(hours=24),  'limit': 500},
            '7d':  {'delta': timedelta(days=7),    'limit': 1000}
        }
        
        if period not in period_map:
            period = '24h'
        
        config = period_map[period]
        end_time = datetime.now()
        start_time = end_time - config['delta']
        
        if limit is None:
            limit = config['limit']
        
        logger.info(f"   🕐 Диапазон: {start_time} → {end_time}")
        logger.info(f"   📊 LIMIT: {limit}")
        
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                # Сначала проверяем, сколько всего записей есть за этот период
                count_query = f"""
                    SELECT COUNT(*) 
                    FROM {self.schema}.{self.table_metrics}
                    WHERE {self.col_tag} = $1
                      AND {self.col_timestamp} >= $2 
                      AND {self.col_timestamp} <= $3
                """
                total_count = await conn.fetchval(count_query, tag_id, start_time, end_time)
                logger.info(f"   📈 Всего записей в БД за период: {total_count}")
                
                if total_count == 0:
                    logger.warning(f"   ⚠️ Нет данных для tag_id={tag_id} за период {period}")
                    return []
                
                # Если записей больше лимита — берем каждую N-ю
                if total_count > limit:
                    step = max(1, total_count // limit)
                    logger.info(f"   🔪 Прореживание: шаг={step} (показываем {total_count // step} из {total_count})")
                    
                    # ✅ ИСПРАВЛЕНО: Используем ts и val в обращении к строкам
                    query = f"""
                        SELECT ts, val FROM (
                            SELECT 
                                {self.col_timestamp} as ts, 
                                {self.col_value} as val,
                                ROW_NUMBER() OVER (ORDER BY {self.col_timestamp}) as rn
                            FROM {self.schema}.{self.table_metrics}
                            WHERE {self.col_tag} = $1
                              AND {self.col_timestamp} >= $2 
                              AND {self.col_timestamp} <= $3
                              AND {self.col_value} IS NOT NULL
                        ) ranked
                        WHERE rn % $4 = 1 OR rn <= 10
                        ORDER BY ts ASC
                    """
                    rows = await conn.fetch(query, tag_id, start_time, end_time, step)
                    
                    # ✅ Возвращаем как (timestamp, value)
                    return [(row['ts'], row['val']) for row in rows]
                else:
                    # Если записей меньше лимита — берем все
                    query = f"""
                        SELECT {self.col_timestamp}, {self.col_value}
                        FROM {self.schema}.{self.table_metrics}
                        WHERE {self.col_tag} = $1
                          AND {self.col_timestamp} >= $2 
                          AND {self.col_timestamp} <= $3
                          AND {self.col_value} IS NOT NULL
                        ORDER BY {self.col_timestamp} ASC
                        LIMIT $4
                    """
                    rows = await conn.fetch(query, tag_id, start_time, end_time, limit)
                    
                    # ✅ Возвращаем как (timestamp, value)
                    return [(row[0], row[1]) for row in rows]
                
        except Exception as e:
            logger.error(f"   ❌ Ошибка запроса: {e}")
            return []
    
    async def get_metric_unit(self, tag_id: int) -> str:
        units_map = {
            'temperature': '°C', 'pressure': 'гПа', 'cpu_load': '%',
            'light': 'лк', 'humidity': '%', 'CO2': 'ppm', 'CPU': '%', 'RAM': '%',
            'Flap': '%', 'Storage': 'ГБ'
        }
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                tag_name = await conn.fetchval(
                    f"SELECT {self.col_tag_name} FROM {self.schema}.{self.table_tags} WHERE {self.col_tag} = $1",
                    tag_id
                )
                if tag_name:
                    for key, unit in units_map.items():
                        if key.lower() in tag_name.lower():
                            return unit
        except:
            pass
        return ''
    
    async def get_available_tags(self, limit: int = 200, search: str = None, with_data_only: bool = True):
        logger.info(f"📋 Получение тегов (limit={limit}, search={search}, with_data={with_data_only})...")
        
        if with_data_only:
            if search:
                query = f"""
                    SELECT DISTINCT td.{self.col_tag}, td.{self.col_tag_name}, COUNT(tv.id) as cnt
                    FROM {self.schema}.{self.table_tags} td
                    INNER JOIN {self.schema}.{self.table_metrics} tv ON td.{self.col_tag} = tv.{self.col_tag}
                    WHERE td.{self.col_tag_name} ILIKE $1
                      AND tv.date_created >= NOW() - INTERVAL '7 days'
                    GROUP BY td.{self.col_tag}, td.{self.col_tag_name}
                    ORDER BY cnt DESC
                    LIMIT $2
                """
                params = (f"%{search}%", limit)
            else:
                query = f"""
                    SELECT DISTINCT td.{self.col_tag}, td.{self.col_tag_name}, COUNT(tv.id) as cnt
                    FROM {self.schema}.{self.table_tags} td
                    INNER JOIN {self.schema}.{self.table_metrics} tv ON td.{self.col_tag} = tv.{self.col_tag}
                    WHERE tv.date_created >= NOW() - INTERVAL '7 days'
                    GROUP BY td.{self.col_tag}, td.{self.col_tag_name}
                    ORDER BY cnt DESC
                    LIMIT $1
                """
                params = (limit,)
        else:
            if search:
                query = f"""
                    SELECT {self.col_tag}, {self.col_tag_name}
                    FROM {self.schema}.{self.table_tags}
                    WHERE {self.col_tag_name} ILIKE $1
                    ORDER BY {self.col_tag_name}
                    LIMIT $2
                """
                params = (f"%{search}%", limit)
            else:
                query = f"""
                    SELECT {self.col_tag}, {self.col_tag_name}
                    FROM {self.schema}.{self.table_tags}
                    ORDER BY {self.col_tag_name}
                    LIMIT $1
                """
                params = (limit,)
        
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                if with_data_only:
                    tags = [(row[self.col_tag], row[self.col_tag_name], row['cnt']) for row in rows]
                else:
                    tags = [(row[self.col_tag], row[self.col_tag_name]) for row in rows]
                logger.info(f"   ✓ Найдено {len(tags)} тегов")
                return tags
        except Exception as e:
            logger.error(f"   ❌ Ошибка: {e}")
            return []
    
    async def get_tag_id_by_name(self, tag_name: str):
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                tag_id = await conn.fetchval(
                    f"SELECT {self.col_tag} FROM {self.schema}.{self.table_tags} WHERE {self.col_tag_name} ILIKE $1 LIMIT 1",
                    f"%{tag_name}%"
                )
                return tag_id
        except:
            return None
    
    async def close(self):
        if self._pool:
            await self._pool.close()
            logger.info("🗄️ Пул подключений закрыт")

_db = None

def get_db():
    global _db
    if _db is None:
        _db = DatabaseConnector()
    return _db

def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()