# Парсер Адресов Arkham

Проект для сбора и хранения данных из Arkham API. Приложение извлекает адреса криптовалютных кошельков с тегами и сохраняет их как в текстовый файл, так и в PostgreSQL базу данных.

## Структура проекта

```
arkham/
├── data/              # Директория для данных
│   ├── .gitkeep
│   └── full_tags_by_type.json    # Файл с типами тегов (необходим для работы)
├── logs/              # Директория для логов
│   └── .gitkeep
├── .env               # Файл с переменными окружения
├── .gitignore         # Файл для игнорирования временных файлов
├── Dockerfile         # Файл для сборки Docker образа
├── docker-compose.yml # Файл для запуска контейнеров
├── models.py          # Модуль для работы с базой данных
├── parser.py          # Основной модуль парсера
└── requirements.txt   # Зависимости Python
```

## Настройка и запуск

### Предварительные требования

- Docker и Docker Compose
- Git

### Шаги для запуска

1. Клонировать репозиторий:
   ```bash
   git clone <url-репозитория>
   cd arkham
   ```

2. Поместите файл `full_tags_by_type.json` в директорию `data/`
   
3. Настройте `.env` файл (по умолчанию уже настроен):
   ```
   # Настройки базы данных
   DB_HOST=postgres
   DB_PORT=5432
   # ... и другие настройки
   ```

4. Запустите приложение с помощью Docker Compose:
   ```bash
   docker-compose up -d
   ```

5. Просмотр логов:
   ```bash
   docker-compose logs -f parser
   ```

## Структура базы данных

База данных состоит из следующих таблиц:

- `addresses` - хранит информацию о криптовалютных адресах
- `tags` - хранит информацию о тегах
- `tag_categories` - хранит категории тегов
- `address_tags` - связующая таблица между адресами и тегами

### Схема базы данных

```
addresses
├── id (PK)
├── address
├── name
├── chain
├── entity_type
├── created_at
└── updated_at

tags
├── id (PK)
├── tag
├── link
├── category_id (FK -> tag_categories.id)
└── created_at

tag_categories
├── id (PK)
├── name
└── created_at

address_tags
├── id (PK)
├── address_id (FK -> addresses.id)
├── tag_id (FK -> tags.id)
└── created_at
```

## Мониторинг и отладка

- Файлы логов находятся в директории `logs/`
- Прогресс обработки сохраняется в файл `data/arkham_progress.json`
- Результаты парсинга сохраняются в файл `data/arkham_addresses.txt` 