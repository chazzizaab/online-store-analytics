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
    {"id": 1, "name": "Смартфон", "category": "Электроника", "price": 29990.0},
    {"id": 2, "name": "Ноутбук", "category": "Электроника", "price": 74990.0},
    {"id": 3, "name": "Кроссовки", "category": "Одежда и обувь", "price": 5490.0},
    {"id": 4, "name": "Кофемашина", "category": "Бытовая техника", "price": 21990.0},
    {"id": 5, "name": "Книга", "category": "Книги", "price": 890.0},
]
CITIES = ["Владивосток", "Москва", "Новосибирск", "Сочи", "Калининград"]


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
        'city': random.choice(CITIES),
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