import requests
import json
import time
import logging
from typing import Dict, Any, List
from datetime import datetime
import os
from models import Database, ArkhamRepository, init_database
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# Настройка логирования
logging_level = os.getenv("LOG_LEVEL", "INFO")
log_file = os.getenv("LOG_FILE", "arkham_parser.log")

logging.basicConfig(
    level=getattr(logging, logging_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def format_tags_from_array(tags_array: List[Dict[str, Any]]) -> str:
    """Форматирует массив тегов в строку."""
    if not tags_array:
        return "Нет тегов"
    return ", ".join([tag.get('label', '') for tag in tags_array])

def extract_tags(tags_array: List[Dict[str, Any]], tag_categories: Dict[str, str]) -> List[Dict[str, str]]:
    """Извлекает теги из массива и добавляет категорию."""
    result = []
    for tag in tags_array:
        tag_link = tag.get('id')
        tag_label = tag.get('label')
        
        if tag_link and tag_label:
            category = tag_categories.get(tag_link, "Other")
            result.append({
                "tag": tag_label,
                "link": tag_link,
                "category": category
            })
    
    return result

def load_progress(progress_file: str) -> Dict[str, bool]:
    """Загружает прогресс обработки тегов из файла."""
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Ошибка при загрузке прогресса: {str(e)}")
    return {}

def save_progress(progress_file: str, progress: Dict[str, bool]):
    """Сохраняет прогресс обработки тегов в файл."""
    try:
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
    except Exception as e:
        logging.error(f"Ошибка при сохранении прогресса: {str(e)}")

def create_tag_categories_map(tags_data) -> Dict[str, str]:
    """Создает маппинг link -> category для тегов."""
    result = {}
    for category, tags in tags_data.items():
        for tag in tags:
            link = tag.get('link')
            if link:
                result[link] = category
    return result

def get_arkham_tag_data(tag_link, page):
    """
    Получает данные из API Arkham для указанного тега и страницы.
    
    Args:
        tag_link (str): Ссылка на тег для получения данных.
        page (int): Номер страницы результатов.
    
    Returns:
        tuple: (dict с данными, boolean флаг успеха)
    """
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,ru;q=0.8",
        "origin": "https://intel.arkm.com",
        "referer": "https://intel.arkm.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "x-payload": os.getenv("ARKHAM_PAYLOAD", "63384b56cf7bd9210dd4fb70ab42dce534c48adafafba56fc6b380d9c329d2f6"),
        "x-timestamp": os.getenv("ARKHAM_TIMESTAMP", "1746752706")
    }

    cookies = {
        "arkham_is_authed": "true",
        "arkham_platform_session": os.getenv("ARKHAM_SESSION", "c8f12120-9264-4703-83b2-70c05fc32012")
    }
    
    # Получаем ограничения API из переменных окружения
    max_retries = int(os.getenv("API_MAX_RETRIES", "3"))
    retry_delay = int(os.getenv("API_RETRY_DELAY", "5"))
    request_timeout = int(os.getenv("API_REQUEST_TIMEOUT", "30"))
    
    url = f"https://api.arkm.com/tag/top?tag={tag_link}&page={page}"
    
    for retry in range(max_retries):
        try:
            logging.info(f"Запрос данных для тега {tag_link}, страница {page}...")
            response = requests.get(
                url, 
                headers=headers, 
                cookies=cookies, 
                timeout=request_timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Проверка на пустой ответ
            if 'addresses' not in data:
                logging.warning(f"Ответ API не содержит ключ 'addresses'. Ключи: {list(data.keys())}")
                if retry < max_retries - 1:
                    logging.warning(f"Повторная попытка {retry+1}/{max_retries} через {retry_delay} сек...")
                    time.sleep(retry_delay)
                    continue
            
            # Небольшая пауза между запросами из переменной окружения
            sleep_time = float(os.getenv("API_REQUEST_DELAY", "1.0"))
            time.sleep(sleep_time)
            
            return data, True
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                # Rate limiting, длительная пауза
                rate_limit_delay = int(os.getenv("API_RATE_LIMIT_DELAY", "60"))
                logging.warning(f"Превышен лимит запросов API (429). Ожидание {rate_limit_delay} секунд...")
                time.sleep(rate_limit_delay)
            elif retry < max_retries - 1:
                logging.warning(f"HTTP ошибка при запросе: {str(e)}. Повторная попытка {retry+1}/{max_retries} через {retry_delay} сек...")
                time.sleep(retry_delay)
            else:
                logging.error(f"HTTP ошибка после {max_retries} попыток: {str(e)}")
                return {}, False
                
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            if retry < max_retries - 1:
                logging.warning(f"Ошибка запроса: {str(e)}. Повторная попытка {retry+1}/{max_retries} через {retry_delay} сек...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Ошибка запроса после {max_retries} попыток: {str(e)}")
                return {}, False
    
    return {}, False

def process_tag(tag_link, output_file, repository, tag_categories, tags_data):
    """
    Обрабатывает конкретный тег, загружая адреса по нему из API Arkham Intel
    и сохраняя их в базу данных.
    
    Args:
        tag_link (str): Ссылка на тег для обработки.
        output_file (str): Имя файла для вывода результатов (не используется).
        repository (ArkhamRepository): Репозиторий для сохранения данных.
        tag_categories (dict): Словарь маппинга ссылок тегов к их категориям.
        tags_data (dict): Исходные данные с тегами.
    
    Returns:
        int: Количество найденных адресов.
    """
    max_pages = 10  # Всегда запрашиваем только 10 страниц
    total_addresses = 0
    
    # Список для подсчета уникальных адресов
    all_addresses = set()
    
    for page in range(1, max_pages + 1):
        logging.info(f"Обработка страницы {page} из {max_pages} для тега {tag_link}")
        
        # Получаем JSON с данными
        response, is_success = get_arkham_tag_data(tag_link, page)
        
        if not is_success:
            logging.error(f"Не удалось получить данные для тега {tag_link} на странице {page}")
            break
        
        # Получаем адреса в ответе
        address_data = response.get('addresses', [])
        
        # Логирование структуры API ответа
        logging.info(f"API ответ: получено {len(address_data)} адресов на странице {page}")
        if len(address_data) == 0:
            logging.info(f"Ключи в ответе API: {list(response.keys())}")
            break  # Если пустой ответ, прерываем обработку
        else:
            logging.info(f"Первый адрес: {address_data[0] if address_data else 'нет'}")
        
        # Проверяем на уникальность адресов
        current_addresses = set(addr.get('address') for addr in address_data if addr.get('address'))
        new_unique_addresses = current_addresses - all_addresses
        
        logging.info(f"Найдено {len(new_unique_addresses)} новых уникальных адресов из {len(current_addresses)} на странице {page}")
        all_addresses.update(current_addresses)
        
        new_addresses = 0
        existing_addresses = 0
        
        # Обрабатываем каждый адрес
        for addr_data in address_data:
            addr = addr_data.get('address')
            chain = addr_data.get('chain', 'unknown')
            entity_name = addr_data.get('entityName') or addr_data.get('entity', {}).get('name', '')
            entity_type = addr_data.get('entityType') or addr_data.get('entity', {}).get('type', '')
            
            # Получаем теги для адреса
            tags = {}
            
            # Добавляем основной тег из категории
            if tag_categories.get(tag_link):
                category = tag_categories[tag_link]
                # Используем новый формат тегов
                # Получаем имя тега из тега по умолчанию
                current_tag_name = tag_link  # По умолчанию используем сам link как имя тега
                
                # Попробуем найти правильное имя тега в исходных данных
                for tag_type, tags_list in tags_data.items():
                    for tag_obj in tags_list:
                        if tag_obj.get('link') == tag_link:
                            current_tag_name = tag_obj.get('name', tag_link)
                            break
                
                tags[category] = [{
                    'id': tag_link,           # link
                    'name': current_tag_name  # имя тега
                }]
            
            # Получаем дополнительные теги из API
            api_tags = addr_data.get('tags', [])
            # Получаем populatedTags, если есть
            populated_tags = addr_data.get('populatedTags', [])
            
            # Обрабатываем populatedTags, если они есть
            if populated_tags:
                logging.info(f"Найдено {len(populated_tags)} тегов в populatedTags для адреса {addr}")
                
                # Если нет тегов из API, инициализируем api_tags
                if not api_tags:
                    api_tags = []
                
                # Добавляем populatedTags в api_tags
                for ptag in populated_tags:
                    tag_id = ptag.get('id')
                    tag_label = ptag.get('label')
                    
                    if tag_id and tag_label:
                        # Добавляем тег в api_tags, используя label как название тега
                        api_tags.append({
                            'id': tag_id,      # Сохраняем id как link
                            'label': tag_label # Используем label как название тега
                        })
                        logging.debug(f"Добавлен тег из populatedTags: {tag_label} ({tag_id})")
            
            if api_tags:
                logging.info(f"Всего {len(api_tags)} тегов для обработки для адреса {addr}")
                
                # Группируем теги по категориям
                for api_tag in api_tags:
                    tag_id = api_tag.get('id')
                    tag_label = api_tag.get('label')
                    
                    if tag_id and tag_label:
                        # Определяем категорию тега: из тех, что известны нам, или "API_Tags"
                        category = tag_categories.get(tag_id, "API_Tags")
                        
                        if category not in tags:
                            tags[category] = []
                            
                        # Вместо добавления просто tag_id, создаем сложную структуру с id и label
                        tags[category].append({
                            'id': tag_id,       # link будет использовать id
                            'name': tag_label   # name будет использовать label
                        })
                        logging.debug(f"Добавлен тег: {tag_label} ({tag_id}) в категорию {category}")
            
            # Если адрес найден, сохраняем его
            if addr:
                total_addresses += 1
                
                # Пытаемся сохранить адрес в БД
                try:
                    result = repository.save_address(addr, chain, entity_name, entity_type)
                    # Сохраняем теги для адреса (в любом случае, даже если адрес уже существовал)
                    repository.save_tags(addr, tags)
                    
                    if result is not None:
                        new_addresses += 1
                    else:
                        existing_addresses += 1
                except Exception as e:
                    logging.error(f"Ошибка при сохранении адреса {addr}: {str(e)}")
        
        logging.info(f"Страница {page}: сохранено {new_addresses} новых и {existing_addresses} существующих адресов")
    
    logging.info(f"Обработка тега {tag_link} завершена. Всего адресов: {total_addresses}")
    return total_addresses

def main():
    # Пути к файлам из переменных окружения
    tags_file = os.getenv("TAGS_FILE", "data/full_tags_by_type.json")
    output_file = os.getenv("OUTPUT_FILE", "data/output.txt")
    progress_file = os.getenv("PROGRESS_FILE", "data/progress.json")
    
    # Создаем директории для файлов, если они не существуют
    os.makedirs(os.path.dirname(tags_file), exist_ok=True)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(progress_file), exist_ok=True)
    
    try:
        # Инициализируем БД
        init_database()
        
        # Создаем подключение к БД
        db = Database()
        repository = ArkhamRepository(db)
        
        # Выводим статус пула соединений
        db.get_pool_status()
        
        # Загружаем прогресс
        progress = load_progress(progress_file)
        
        # Читаем файл с тегами
        with open(tags_file, 'r', encoding='utf-8') as f:
            tags_data = json.load(f)
        
        # Сохраняем категории тегов в БД
        repository.save_tag_categories(tags_data)
        
        # Создаем маппинг link -> category для тегов
        tag_categories = create_tag_categories_map(tags_data)
        
        total_tags = 0
        total_addresses = 0
        
        # Получаем общее количество тегов для статистики
        all_tags = []
        for tag_type, tags in tags_data.items():
            for tag in tags:
                tag_link = tag.get('link')
                if tag_link:
                    all_tags.append(tag_link)
        
        # Выводим статистику по прогрессу
        tags_completed = sum(1 for tag in all_tags if progress.get(tag, False))
        tags_remaining = len(all_tags) - tags_completed
        logging.info(f"Всего тегов: {len(all_tags)}")
        logging.info(f"Уже обработано: {tags_completed}")
        logging.info(f"Осталось обработать: {tags_remaining}")
        
        # Счетчик операций для периодического вывода статуса пула
        operation_count = 0
        
        # Обрабатываем каждый тип тегов
        for tag_type, tags in tags_data.items():
            logging.info(f"\nОбработка типа тегов: {tag_type}")
            
            for tag in tags:
                tag_link = tag.get('link')
                tag_name = tag.get('name')
                
                if tag_link:
                    # Проверяем, был ли тег уже обработан
                    if progress.get(tag_link, False):
                        logging.info(f"Тег {tag_name} ({tag_link}) уже обработан, пропускаем.")
                        continue
                    
                    # Периодически проверяем статус пула соединений
                    operation_count += 1
                    if operation_count % 10 == 0:  # Каждые 10 операций
                        db.get_pool_status()
                    
                    logging.info(f"\nОбрабатываем тег: {tag_name} ({tag_link})")
                    
                    try:
                        addresses_count = process_tag(tag_link, output_file, repository, tag_categories, tags_data)
                        total_tags += 1
                        total_addresses += addresses_count
                        
                        # Отмечаем тег как обработанный и сохраняем прогресс
                        progress[tag_link] = True
                        save_progress(progress_file, progress)
                        
                        logging.info(f"Тег {tag_name} ({tag_link}) обработан и сохранен в прогрессе.")
                    except Exception as e:
                        logging.error(f"Ошибка при обработке тега {tag_name} ({tag_link}): {str(e)}")
                        # При ошибке проверяем состояние пула и пытаемся восстановить его
                        db.get_pool_status()
                        # Сохраняем прогресс на случай падения
                        save_progress(progress_file, progress)
        
        logging.info(f"\n✅ Обработка завершена.")
        logging.info(f"Всего обработано тегов в этом запуске: {total_tags}")
        logging.info(f"Всего найдено адресов в этом запуске: {total_addresses}")
        logging.info(f"Всего обработано тегов: {sum(1 for tag in all_tags if progress.get(tag, False))}")
        
        # Закрываем подключение к базе данных
        db.close()
        
    except Exception as e:
        logging.error(f"❌ Ошибка: {str(e)}")

if __name__ == "__main__":
    main()