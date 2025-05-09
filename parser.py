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

def process_tag(tag_link: str, output_file: str, repository: ArkhamRepository, tag_categories: Dict[str, str]) -> int:
    """Обрабатывает все страницы для одного тега и сохраняет результат в базу данных и файл."""
    page = 1
    total_addresses = 0
    has_more_data = True
    
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
    
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write(f"\n\n=== ТЕГ: {tag_link} ===\n")
    
    # Получаем ограничения API из переменных окружения
    max_retries = int(os.getenv("API_MAX_RETRIES", "3"))
    retry_delay = int(os.getenv("API_RETRY_DELAY", "5"))
    request_timeout = int(os.getenv("API_REQUEST_TIMEOUT", "30"))
    
    while has_more_data:
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
                addresses = data.get('addresses', [])
                
                # Если нет адресов или их меньше ожидаемого количества, завершаем обработку тега
                if not addresses:
                    has_more_data = False
                    logging.info(f"Данные для тега {tag_link} закончились на странице {page-1}.")
                    break
                    
                # Добавляем адреса в файл и базу данных
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f"\n-- Страница {page} --\n")
                    for addr in addresses:
                        address = addr.get('address', 'N/A')
                        chain = addr.get('chain', 'unknown')
                        entity = addr.get('arkhamEntity', {})
                        name = entity.get('name', 'N/A')
                        entity_type = entity.get('type', '')
                        
                        # Извлекаем и форматируем теги для вывода в файл
                        tags_str = format_tags_from_array(addr.get('populatedTags', []))
                        
                        # Извлекаем теги для сохранения в БД
                        tags = extract_tags(addr.get('populatedTags', []), tag_categories)
                        
                        # Записываем в файл
                        f.write(f"{address} - {name} - {tags_str}\n")
                        
                        # Сохраняем в БД
                        address_data = {
                            'address': address,
                            'name': name,
                            'chain': chain,
                            'entity_type': entity_type,
                            'tags': tags
                        }
                        try:
                            repository.save_address(address_data)
                        except Exception as e:
                            logging.error(f"Ошибка при сохранении адреса {address} в БД: {str(e)}")
                
                total_addresses += len(addresses)
                logging.info(f"Обработано {len(addresses)} адресов для тега {tag_link}, страница {page}")
                
                # Переходим к следующей странице
                page += 1
                
                # Небольшая пауза между запросами из переменной окружения
                sleep_time = float(os.getenv("API_REQUEST_DELAY", "1.0"))
                time.sleep(sleep_time)
                
                # Успешный запрос, выходим из цикла повторных попыток
                break
                
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
                    has_more_data = False
                    break
                    
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                if retry < max_retries - 1:
                    logging.warning(f"Ошибка запроса: {str(e)}. Повторная попытка {retry+1}/{max_retries} через {retry_delay} сек...")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"Ошибка запроса после {max_retries} попыток: {str(e)}")
                    has_more_data = False
                    break
    
    return total_addresses

def main():
    # Пути к файлам из переменных окружения
    tags_file = os.getenv("TAGS_FILE", "full_tags_by_type.json")
    output_file = os.getenv("OUTPUT_FILE", "arkham_addresses.txt")
    progress_file = os.getenv("PROGRESS_FILE", "arkham_progress.json")
    
    # Убедимся, что директории для файлов существуют
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)
    os.makedirs(os.path.dirname(progress_file) if os.path.dirname(progress_file) else ".", exist_ok=True)
    
    try:
        # Инициализируем БД
        init_database()
        
        # Создаем подключение к БД
        db = Database()
        repository = ArkhamRepository(db)
        
        # Загружаем прогресс
        progress = load_progress(progress_file)
        
        # Если файл с результатами не существует или все теги не обработаны,
        # создаем или дополняем файл результатов
        if not os.path.exists(output_file) or not progress:
            with open(output_file, 'a', encoding='utf-8') as f:
                f.write(f"АДРЕСА ARKHAM\nВремя запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("Формат: адрес - имя - теги\n")
                f.write("-"*80 + "\n")
        
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
                    
                    logging.info(f"\nОбрабатываем тег: {tag_name} ({tag_link})")
                    addresses_count = process_tag(tag_link, output_file, repository, tag_categories)
                    total_tags += 1
                    total_addresses += addresses_count
                    
                    # Отмечаем тег как обработанный и сохраняем прогресс
                    progress[tag_link] = True
                    save_progress(progress_file, progress)
                    
                    logging.info(f"Тег {tag_name} ({tag_link}) обработан и сохранен в прогрессе.")
        
        # Добавляем статистику в конец файла
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write("\n\n" + "="*80 + "\n")
            f.write(f"ИТОГИ:\n")
            f.write(f"Всего обработано тегов: {total_tags}\n")
            f.write(f"Всего найдено адресов: {total_addresses}\n")
            f.write(f"Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        logging.info(f"\n✅ Обработка завершена. Результаты сохранены в файл: {output_file}")
        logging.info(f"Всего обработано тегов в этом запуске: {total_tags}")
        logging.info(f"Всего найдено адресов в этом запуске: {total_addresses}")
        logging.info(f"Всего обработано тегов: {sum(1 for tag in all_tags if progress.get(tag, False))}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка: {str(e)}")

if __name__ == "__main__":
    main()