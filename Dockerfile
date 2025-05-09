FROM python:3.11-slim

WORKDIR /app

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Создаем директории для данных и логов
RUN mkdir -p /app/data /app/logs

# Копируем папку data
COPY data/ /app/data/
# Проверка содержимого копированной директории
RUN echo "Содержимое директории data:" && ls -la /app/data/

# Копируем код приложения
COPY *.py .
COPY .env .

# Запускаем парсер
CMD ["python", "parser.py"] 