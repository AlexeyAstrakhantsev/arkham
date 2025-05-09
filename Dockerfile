FROM python:3.11-slim

WORKDIR /app

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Создаем директории для данных и логов
RUN mkdir -p /app/data /app/logs

# Проверяем директорию перед копированием
RUN echo "Исходная директория data пуста? $(if [ -z "$(ls -A data 2>/dev/null)" ]; then echo 'Да'; else echo 'Нет'; fi)"

# Копируем папку data
COPY data/ /app/data/

# Проверка содержимого копированной директории
RUN echo "Содержимое директории /app/data:" && ls -la /app/data/ || echo "Директория пуста или не существует"
RUN find /app -type f -name "full_tags_by_type.json" || echo "Файл full_tags_by_type.json не найден"

# Копируем код приложения
COPY *.py .
COPY .env .

# Запускаем парсер
CMD ["python", "parser.py"] 