from datetime import datetime
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
import logging
import json
import os
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional

# Загружаем переменные окружения из .env файла
load_dotenv()

# Настройка логирования
logging_level = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, logging_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class Database:
    """Класс для работы с базой данных PostgreSQL с использованием пула соединений."""
    
    def __init__(self):
        """Инициализирует подключение к базе данных из переменных окружения."""
        self.config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "arkham_db"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgres")
        }
        
        # Минимальное и максимальное количество соединений в пуле
        min_connections = int(os.getenv("DB_MIN_CONNECTIONS", "1"))
        max_connections = int(os.getenv("DB_MAX_CONNECTIONS", "10"))
        
        # Создаем пул соединений
        self.pool = psycopg2.pool.SimpleConnectionPool(
            min_connections,
            max_connections,
            **self.config
        )
        logging.info(f"Создан пул соединений к базе данных на {self.config['host']}:{self.config['port']}")
    
    def get_connection(self):
        """Получает соединение из пула."""
        return self.pool.getconn()
    
    def release_connection(self, conn):
        """Возвращает соединение в пул."""
        self.pool.putconn(conn)
    
    def execute_query(self, query, params=None, fetch=False, fetch_one=False):
        """Выполняет SQL запрос с возможностью получения результатов."""
        conn = None
        cursor = None
        result = None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(query, params)
            
            if fetch:
                result = cursor.fetchall()
            elif fetch_one:
                result = cursor.fetchone()
                
            conn.commit()
            return result
            
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Ошибка при выполнении запроса: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.release_connection(conn)
    
    def close(self):
        """Закрывает все соединения в пуле."""
        if hasattr(self, 'pool') and self.pool:
            self.pool.closeall()
            logging.info("Пул соединений закрыт")


class ArkhamRepository:
    """Репозиторий для работы с данными Arkham в базе данных."""
    
    def __init__(self, db: Database):
        """Инициализирует репозиторий с экземпляром базы данных."""
        self.db = db
    
    def save_address(self, address_data: Dict[str, Any]) -> Optional[int]:
        """Сохраняет адрес и связанные с ним теги в базу данных."""
        try:
            # Проверяем, существует ли адрес
            check_query = """
            SELECT id FROM addresses WHERE address = %s AND chain = %s
            """
            address_id = self.db.execute_query(
                check_query, 
                (address_data['address'], address_data['chain']), 
                fetch_one=True
            )
            
            if address_id:
                address_id = address_id[0]
                # Обновляем существующий адрес
                update_query = """
                UPDATE addresses 
                SET name = %s, entity_type = %s, updated_at = NOW()
                WHERE id = %s
                """
                self.db.execute_query(
                    update_query, 
                    (address_data['name'], address_data['entity_type'], address_id)
                )
            else:
                # Вставляем новый адрес
                insert_query = """
                INSERT INTO addresses (address, name, chain, entity_type, created_at, updated_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                RETURNING id
                """
                address_id = self.db.execute_query(
                    insert_query, 
                    (
                        address_data['address'], 
                        address_data['name'], 
                        address_data['chain'], 
                        address_data['entity_type']
                    ), 
                    fetch_one=True
                )[0]
            
            # Сохраняем теги для адреса
            if 'tags' in address_data and address_data['tags']:
                for tag_data in address_data['tags']:
                    self._save_tag_for_address(address_id, tag_data)
            
            return address_id
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении адреса {address_data['address']}: {str(e)}")
            raise
    
    def _save_tag_for_address(self, address_id: int, tag_data: Dict[str, str]):
        """Сохраняет тег и его связь с адресом."""
        try:
            # Проверяем, существует ли тег
            check_query = """
            SELECT id FROM tags WHERE link = %s
            """
            tag_id = self.db.execute_query(check_query, (tag_data['link'],), fetch_one=True)
            
            if not tag_id:
                # Получаем ID категории
                category_query = """
                SELECT id FROM tag_categories WHERE name = %s
                """
                category_id = self.db.execute_query(category_query, (tag_data['category'],), fetch_one=True)
                
                if not category_id:
                    # Если категории нет, создаем ее
                    insert_category_query = """
                    INSERT INTO tag_categories (name, created_at)
                    VALUES (%s, NOW())
                    RETURNING id
                    """
                    category_id = self.db.execute_query(insert_category_query, (tag_data['category'],), fetch_one=True)[0]
                else:
                    category_id = category_id[0]
                
                # Вставляем новый тег
                insert_tag_query = """
                INSERT INTO tags (tag, link, category_id, created_at)
                VALUES (%s, %s, %s, NOW())
                RETURNING id
                """
                tag_id = self.db.execute_query(
                    insert_tag_query, 
                    (tag_data['tag'], tag_data['link'], category_id), 
                    fetch_one=True
                )[0]
            else:
                tag_id = tag_id[0]
            
            # Проверяем, существует ли связь адрес-тег
            check_relation_query = """
            SELECT id FROM address_tags WHERE address_id = %s AND tag_id = %s
            """
            relation = self.db.execute_query(
                check_relation_query, 
                (address_id, tag_id), 
                fetch_one=True
            )
            
            if not relation:
                # Создаем связь адрес-тег
                insert_relation_query = """
                INSERT INTO address_tags (address_id, tag_id, created_at)
                VALUES (%s, %s, NOW())
                """
                self.db.execute_query(insert_relation_query, (address_id, tag_id))
                
        except Exception as e:
            logging.error(f"Ошибка при сохранении тега {tag_data['tag']} для адреса {address_id}: {str(e)}")
            raise
    
    def save_tag_categories(self, categories_data: Dict[str, List[Dict[str, str]]]):
        """Сохраняет категории тегов из JSON файла."""
        try:
            for category_name, tags in categories_data.items():
                # Проверяем существование категории
                check_query = """
                SELECT id FROM tag_categories WHERE name = %s
                """
                category_id = self.db.execute_query(check_query, (category_name,), fetch_one=True)
                
                if not category_id:
                    # Создаем новую категорию
                    insert_query = """
                    INSERT INTO tag_categories (name, created_at)
                    VALUES (%s, NOW())
                    RETURNING id
                    """
                    category_id = self.db.execute_query(insert_query, (category_name,), fetch_one=True)[0]
                else:
                    category_id = category_id[0]
                
                # Сохраняем все теги для данной категории
                for tag in tags:
                    tag_name = tag.get('name')
                    tag_link = tag.get('link')
                    
                    if tag_name and tag_link:
                        # Проверяем существование тега
                        check_tag_query = """
                        SELECT id FROM tags WHERE link = %s
                        """
                        tag_id = self.db.execute_query(check_tag_query, (tag_link,), fetch_one=True)
                        
                        if not tag_id:
                            # Создаем новый тег
                            insert_tag_query = """
                            INSERT INTO tags (tag, link, category_id, created_at)
                            VALUES (%s, %s, %s, NOW())
                            """
                            self.db.execute_query(insert_tag_query, (tag_name, tag_link, category_id))
                            
        except Exception as e:
            logging.error(f"Ошибка при сохранении категорий тегов: {str(e)}")
            raise


def init_database():
    """Инициализирует базу данных, создавая необходимые таблицы."""
    # Получаем настройки подключения из переменных окружения
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "arkham_db"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres")
    }
    
    conn = None
    
    try:
        logging.info(f"Подключение к базе данных на {db_config['host']}:{db_config['port']} ...")
        
        # Пытаемся подключиться к базе данных
        try:
            conn = psycopg2.connect(**db_config)
            conn.autocommit = True
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                # База данных не существует, создаем ее
                db_config_temp = db_config.copy()
                db_config_temp["database"] = "postgres"  # Используем стандартную базу postgres для подключения
                
                logging.info(f"База данных {db_config['database']} не существует. Создаем...")
                
                conn_temp = psycopg2.connect(**db_config_temp)
                conn_temp.autocommit = True
                cursor_temp = conn_temp.cursor()
                
                # Создаем базу данных
                cursor_temp.execute(f"CREATE DATABASE {db_config['database']}")
                
                cursor_temp.close()
                conn_temp.close()
                
                # Подключаемся к новой базе данных
                conn = psycopg2.connect(**db_config)
                conn.autocommit = True
            else:
                raise
        
        cursor = conn.cursor()
        
        # Создаем таблицу для категорий тегов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tag_categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE,
            created_at TIMESTAMP NOT NULL
        )
        """)
        
        # Создаем таблицу для тегов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            id SERIAL PRIMARY KEY,
            tag VARCHAR(255) NOT NULL,
            link VARCHAR(255) NOT NULL UNIQUE,
            category_id INTEGER REFERENCES tag_categories(id),
            created_at TIMESTAMP NOT NULL
        )
        """)
        
        # Создаем таблицу для адресов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS addresses (
            id SERIAL PRIMARY KEY,
            address VARCHAR(255) NOT NULL,
            name VARCHAR(255),
            chain VARCHAR(50) NOT NULL,
            entity_type VARCHAR(100),
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            UNIQUE(address, chain)
        )
        """)
        
        # Создаем таблицу для связи адресов и тегов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS address_tags (
            id SERIAL PRIMARY KEY,
            address_id INTEGER REFERENCES addresses(id),
            tag_id INTEGER REFERENCES tags(id),
            created_at TIMESTAMP NOT NULL,
            UNIQUE(address_id, tag_id)
        )
        """)
        
        # Создаем индексы для оптимизации запросов
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_addresses_address ON addresses(address)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_addresses_chain ON addresses(chain)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tags_link ON tags(link)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tags_category ON tags(category_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_tags_address ON address_tags(address_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_tags_tag ON address_tags(tag_id)")
        
        logging.info("База данных успешно инициализирована")
        
    except Exception as e:
        logging.error(f"Ошибка при инициализации базы данных: {str(e)}")
        raise
    finally:
        if conn:
            conn.close() 