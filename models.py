from datetime import datetime
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
import logging
import json
import os
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
import time

# Загружаем переменные окружения из .env файла
load_dotenv()

# Настройка логирования
logging_level = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, logging_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class Database:
    """
    Класс для работы с базой данных PostgreSQL без использования пула соединений.
    """
    def __init__(self, host, port, user, password, dbname):
        """
        Инициализирует подключение к базе данных.
        """
        self.config = {
            "host": host,
            "port": port,
            "database": dbname,
            "user": user,
            "password": password
        }
        self.connection = None

    def connect(self):
        """
        Устанавливает соединение с базой данных.
        """
        if self.connection is None:
            try:
                self.connection = psycopg2.connect(**self.config)
                logging.info(f"Установлено соединение с базой данных на {self.config['host']}:{self.config['port']}")
            except psycopg2.Error as e:
                logging.error(f"Ошибка подключения к базе данных: {str(e)}")
                raise

    def get_connection(self):
        """
        Возвращает текущее соединение с базой данных.
        """
        if self.connection is None:
            self.connect()
        return self.connection

    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        if self.connection is not None:
            try:
                self.connection.close()
                logging.info("Соединение с базой данных закрыто")
            except psycopg2.Error as e:
                logging.error(f"Ошибка при закрытии соединения с базой данных: {str(e)}")
            finally:
                self.connection = None

    def execute_query(self, query, params=None, fetch=False, fetch_one=False):
        """
        Выполняет SQL запрос с возможностью получения результатов.
        
        Args:
            query (str): SQL запрос для выполнения.
            params (tuple): Параметры для SQL запроса.
            fetch (bool): Если True, возвращает все результаты.
            fetch_one (bool): Если True, возвращает один результат.
        
        Returns:
            list или tuple: Результаты запроса, если fetch или fetch_one установлены в True.
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                if fetch_one:
                    return cursor.fetchone()
                conn.commit()
        except Exception as e:
            logging.error(f"Ошибка при выполнении SQL запроса: {str(e)}")
            raise


class ArkhamRepository:
    """Репозиторий для работы с данными Arkham в базе данных."""
    
    def __init__(self, db: Database):
        """Инициализирует репозиторий с экземпляром базы данных."""
        self.db = db
    
    def save_address(self, address, chain, entity_name, entity_type):
        """
        Сохраняет адрес в базу данных и, если tag_unified не пустой, в таблицу unified_addresses.
        
        Args:
            address (str): Адрес кошелька.
            chain (str): Блокчейн (сеть).
            entity_name (str): Название сущности.
            entity_type (str): Тип сущности.
            
        Returns:
            str: ID добавленного адреса или None, если адрес уже существовал.
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                # Проверка, существует ли адрес уже
                cursor.execute(
                    "SELECT id FROM addresses WHERE address = %s",
                    (address,)
                )
                result = cursor.fetchone()
                
                if result:
                    # Если адрес уже существует, обновляем информацию
                    cursor.execute(
                        """
                        UPDATE addresses 
                        SET chain = %s, entity_name = %s, entity_type = %s, updated_at = NOW()
                        WHERE address = %s
                        """,
                        (chain, entity_name, entity_type, address)
                    )
                    conn.commit()
                    address_id = result[0]
                else:
                    # Если адрес не существует, добавляем новый
                    cursor.execute(
                        """
                        INSERT INTO addresses (address, chain, entity_name, entity_type, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, NOW(), NOW())
                        RETURNING id
                        """,
                        (address, chain, entity_name, entity_type)
                    )
                    address_id = cursor.fetchone()[0]
                    conn.commit()
                
                # Получаем все теги для адреса и проверяем их tag_unified
                cursor.execute("""
                    SELECT DISTINCT t.tag_unified 
                    FROM tags t
                    JOIN address_tags at ON t.id = at.tag_id
                    JOIN addresses a ON at.address_id = a.id
                    WHERE a.address = %s AND t.tag_unified IS NOT NULL
                """, (address,))
                
                tag_unified_results = cursor.fetchall()
                
                if tag_unified_results:
                    # Если есть теги с tag_unified, сохраняем в unified_addresses
                    for tag_unified in tag_unified_results:
                        cursor.execute(
                            """
                            INSERT INTO unified_addresses (address, type, address_name, labels, source, created_at)
                            VALUES (%s, %s, %s, '{}', 'akhram-tags', NOW())
                            """,
                            (address, tag_unified[0], entity_name)
                        )
                    conn.commit()
                    logging.info(f"Адрес {address} сохранен в unified_addresses с типами {[t[0] for t in tag_unified_results]}")
                
                return None if result else address_id  # Возвращаем None для обновленного адреса или ID для нового
        except Exception as e:
            logging.error(f"Ошибка при сохранении адреса {address}: {str(e)}")
            raise e

    def save_tags(self, address, tags_dict):
        """
        Сохраняет теги для указанного адреса.
        
        Args:
            address (str): Адрес кошелька.
            tags_dict (dict): Словарь тегов в формате {категория: [список_тегов]},
                             где каждый тег это словарь {'id': 'тег_id', 'name': 'имя_тега'}.
        """
        if not tags_dict:
            return
            
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Получаем ID адреса
                cursor.execute(
                    "SELECT id FROM addresses WHERE address = %s",
                    (address,)
                )
                address_result = cursor.fetchone()
                
                if not address_result:
                    logging.error(f"Не удалось найти адрес {address} для добавления тегов")
                    return
                    
                address_id = address_result[0]
                
                # Обрабатываем теги по категориям
                for category, tags_list in tags_dict.items():
                    # Убедимся, что категория существует
                    cursor.execute(
                        """
                        INSERT INTO tag_categories (name) 
                        VALUES (%s) 
                        ON CONFLICT (name) DO NOTHING
                        RETURNING id
                        """,
                        (category,)
                    )
                    category_id_result = cursor.fetchone()
                    
                    if category_id_result:
                        category_id = category_id_result[0]
                    else:
                        # Если категория уже существовала, получаем её ID
                        cursor.execute(
                            "SELECT id FROM tag_categories WHERE name = %s",
                            (category,)
                        )
                        category_id = cursor.fetchone()[0]
                    
                    # Обрабатываем теги в категории
                    for tag_item in tags_list:
                        tag_id = tag_item.get('id')
                        tag_name = tag_item.get('name', tag_id)
                        
                        if not tag_id:
                            continue
                            
                        # Сохраняем тег, если он не существует
                        cursor.execute(
                            """
                            INSERT INTO tags (tag_id, name, category_id) 
                            VALUES (%s, %s, %s) 
                            ON CONFLICT (tag_id) DO UPDATE 
                            SET name = EXCLUDED.name, category_id = EXCLUDED.category_id
                            RETURNING id
                            """,
                            (tag_id, tag_name, category_id)
                        )
                        db_tag_id = cursor.fetchone()[0]
                        
                        # Связываем тег с адресом
                        cursor.execute(
                            """
                            INSERT INTO address_tags (address_id, tag_id) 
                            VALUES (%s, %s) 
                            ON CONFLICT (address_id, tag_id) DO NOTHING
                            """,
                            (address_id, db_tag_id)
                        )
                
                conn.commit()
        except Exception as e:
            logging.error(f"Ошибка при сохранении тегов для адреса {address}: {str(e)}")
            raise e

    def save_tag_categories(self, categories_data: Dict[str, List[Dict[str, str]]]):
        """Сохраняет категории тегов из JSON файла."""
        try:
            logging.info(f"Начинаю сохранение категорий тегов в базу данных. Всего категорий: {len(categories_data)}")
            
            for category_name, tags in categories_data.items():
                logging.info(f"Обработка категории: {category_name} (тегов: {len(tags)})")
                
                # Проверяем существование категории
                check_query = """
                SELECT id FROM tag_categories WHERE name = %s
                """
                category_id = self.db.execute_query(check_query, (category_name,), fetch_one=True)
                
                if not category_id:
                    # Создаем новую категорию
                    logging.info(f"Создание новой категории: {category_name}")
                    insert_query = """
                    INSERT INTO tag_categories (name, created_at)
                    VALUES (%s, NOW())
                    RETURNING id
                    """
                    category_id = self.db.execute_query(insert_query, (category_name,), fetch_one=True)[0]
                    logging.info(f"Категория {category_name} создана с ID: {category_id}")
                else:
                    category_id = category_id[0]
                    logging.info(f"Категория {category_name} уже существует с ID: {category_id}")
                
                # Сохраняем все теги для данной категории
                saved_tags = 0
                for tag in tags:
                    tag_name = tag.get('name')
                    tag_link = tag.get('link')
                    
                    if tag_name and tag_link:
                        # Проверяем существование тега
                        check_tag_query = """
                        SELECT id FROM tags WHERE tag_id = %s
                        """
                        tag_id = self.db.execute_query(check_tag_query, (tag_link,), fetch_one=True)
                        
                        if not tag_id:
                            # Создаем новый тег
                            insert_tag_query = """
                            INSERT INTO tags (name, tag_id, category_id, created_at)
                            VALUES (%s, %s, %s, NOW())
                            """
                            self.db.execute_query(insert_tag_query, (tag_name, tag_link, category_id))
                            saved_tags += 1
                            
                logging.info(f"Сохранено {saved_tags} новых тегов для категории {category_name}")
                
            logging.info("Все категории тегов успешно сохранены в базу данных")
                            
        except Exception as e:
            logging.error(f"Ошибка при сохранении категорий тегов: {str(e)}")
            raise


def init_database(db_host, db_port, db_user, db_password, db_name):
    """
    Инициализирует соединение с базой данных и создает необходимые таблицы если их нет.
    
    Args:
        db_host (str): Хост базы данных.
        db_port (int): Порт базы данных.
        db_user (str): Имя пользователя.
        db_password (str): Пароль пользователя.
        db_name (str): Имя базы данных.
        
    Returns:
        Database: Объект для работы с базой данных.
        
    Raises:
        Exception: Если не удается подключиться к базе данных.
    """
    # Попытка подключения
    for attempt in range(5):
        try:
            logging.info(f"Попытка подключения к БД {db_name} на {db_host}:{db_port} (попытка {attempt+1}/5)")
            db = Database(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                dbname=db_name
            )
            
            # Создаем нужные таблицы, если их нет
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Таблица категорий тегов
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS tag_categories (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """)
                
                # Таблица тегов
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    id SERIAL PRIMARY KEY,
                    tag_id VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    tag_unified VARCHAR(50),
                    category_id INTEGER REFERENCES tag_categories(id),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """)
                
                # Таблица адресов
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS addresses (
                    id SERIAL PRIMARY KEY,
                    address VARCHAR(255) UNIQUE NOT NULL,
                    chain VARCHAR(50),
                    entity_name VARCHAR(255),
                    entity_type VARCHAR(100),
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """)
                
                # Таблица связей адрес-тег
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS address_tags (
                    id SERIAL PRIMARY KEY,
                    address_id INTEGER REFERENCES addresses(id),
                    tag_id INTEGER REFERENCES tags(id),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE(address_id, tag_id)
                )
                """)
                
                # Таблица unified_addresses
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS unified_addresses (
                    id SERIAL PRIMARY KEY,
                    address VARCHAR(50) NOT NULL,
                    type VARCHAR(20) NOT NULL,
                    address_name VARCHAR(50),
                    labels JSON,
                    source VARCHAR(50),
                    created_at TIMESTAMP NOT NULL DEFAULT timezone('utc'::text, now())
                )
                """)
                
                conn.commit()
                logging.info("Структура базы данных успешно инициализирована")
            
            return db
            
        except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
            logging.error(f"Ошибка подключения к БД (попытка {attempt+1}): {str(e)}")
            time.sleep(5)  # Ждем 5 секунд перед повторной попыткой
    
    # Если дошли сюда, значит все попытки подключения не удались
    error_msg = f"Не удалось подключиться к базе данных после 5 попыток"
    logging.error(error_msg)
    raise Exception(error_msg) 