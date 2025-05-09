FROM python:3.11-slim

WORKDIR /app

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Создаем директории для данных и логов
RUN mkdir -p /app/data /app/logs

# Копируем код приложения
COPY *.py .
COPY .env .

# Создаем и используем не-root пользователя
RUN useradd -m appuser
RUN chown -R appuser:appuser /app
USER appuser

# Запускаем парсер
CMD ["python", "parser.py"] 