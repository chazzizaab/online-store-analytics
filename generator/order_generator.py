import psycopg2
import time
import random
import os
import logging
from datetime import datetime
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
fake = Faker('ru_RU')

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "database": os.getenv("DB_NAME", "online_store_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "port": int(os.getenv("DB_PORT", 5432))
}

PRODUCTS = [
    {"id": 1, "name": "iPhone 15 Pro 256GB", "category": "Смартфоны", "price": 119990.0},
    {"id": 2, "name": "Samsung Galaxy S24 Ultra", "category": "Смартфоны", "price": 109990.0},
    {"id": 3, "name": "Xiaomi Redmi Note 13 Pro", "category": "Смартфоны", "price": 34990.0},
    {"id": 4, "name": "MacBook Air M3 13\"", "category": "Ноутбуки", "price": 134990.0},
    {"id": 5, "name": "Lenovo IdeaPad 5 Pro", "category": "Ноутбуки", "price": 89990.0},
    {"id": 6, "name": "ASUS ROG Strix G16", "category": "Игровые ноутбуки", "price": 159990.0},
    {"id": 7, "name": "Sony WH-1000XM5", "category": "Наушники", "price": 34990.0},
    {"id": 8, "name": "Apple AirPods Pro 2", "category": "Наушники", "price": 24990.0},
    {"id": 9, "name": "Nike Air Force 1", "category": "Обувь", "price": 12990.0},
    {"id": 10, "name": "Adidas Ultraboost 5.0", "category": "Обувь", "price": 14990.0},
    {"id": 11, "name": "Levi's 501 Original", "category": "Джинсы", "price": 7990.0},
    {"id": 12, "name": "The North Face Jacket", "category": "Одежда", "price": 18990.0},
    {"id": 13, "name": "DeLonghi Magnifica", "category": "Кофемашины", "price": 54990.0},
    {"id": 14, "name": "Philips Airfryer XXL", "category": "Кухонная техника", "price": 12990.0},
    {"id": 15, "name": "Dyson V15 Detect", "category": "Пылесосы", "price": 69990.0},
    {"id": 16, "name": "Книга 'Мастер и Маргарита'", "category": "Книги", "price": 890.0},
    {"id": 17, "name": "Книга '1984' Дж. Оруэлл", "category": "Книги", "price": 750.0},
    {"id": 18, "name": "PlayStation 5 Slim", "category": "Игровые консоли", "price": 64990.0},
    {"id": 19, "name": "Nintendo Switch OLED", "category": "Игровые консоли", "price": 32990.0},
    {"id": 20, "name": "Apple Watch Series 9", "category": "Умные часы", "price": 45990.0},
    {"id": 21, "name": "Canon EOS R50", "category": "Фототехника", "price": 89990.0},
    {"id": 22, "name": "GoPro Hero 12", "category": "Экшн-камеры", "price": 45990.0},
    {"id": 23, "name": "Xiaomi Scooter 4 Pro", "category": "Электротранспорт", "price": 49990.0},
    {"id": 24, "name": "JBL Flip 6", "category": "Портативные колонки", "price": 8990.0},
    {"id": 25, "name": "Samsung Galaxy Watch 6", "category": "Умные часы", "price": 32990.0},
]

CITIES_WEIGHTED = {
    "Москва": 30,
    "Санкт-Петербург": 20,
    "Екатеринбург": 10,
    "Казань": 8,
    "Новосибирск": 7,
    "Нижний Новгород": 5,
    "Воронеж": 4,
    "Уфа": 1,
    "Пермь": 2,
    "Владивосток": 3.5,
    "Сочи": 2.3,
    "Калининград": 4.2
}

def choose_city_weighted():
    """Выбирает город с учетом весов (вероятностей)"""
    cities = list(CITIES_WEIGHTED.keys())
    weights = list(CITIES_WEIGHTED.values())
    return random.choices(cities, weights=weights, k=1)[0]

def create_orders_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                quantity INTEGER,
                city VARCHAR(50),
                order_date TIMESTAMP
            );
        """)
        conn.commit()
        logger.info("Таблица 'orders' создана/проверена.")


def generate_order():
    product = random.choice(PRODUCTS)
    return {
        'product_id': product['id'],
        'product_name': product['name'],
        'category': product['category'],
        'price': product['price'],
        'quantity': random.randint(1, 3),
        'city': choose_city_weighted(),
        'order_date': datetime.now()
    }


def main():
    logger.info("Генератор заказов запущен...")

    # Подключаемся к БД (healthcheck гарантирует готовность)
    conn = psycopg2.connect(**DB_CONFIG)
    create_orders_table(conn)

    try:
        while True:
            order = generate_order()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orders (product_id, product_name, category, price, quantity, city, order_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (order['product_id'], order['product_name'], order['category'],
                      order['price'], order['quantity'], order['city'], order['order_date']))
                conn.commit()
            logger.info(f"Заказ добавлен: {order['product_name']} в {order['city']}")
            time.sleep(random.uniform(1, 3))
    except KeyboardInterrupt:
        logger.info("Остановка генератора.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()