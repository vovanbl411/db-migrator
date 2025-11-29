# Этап 1: Сборка приложения
FROM golang:1.25-alpine AS builder

# Устанавливаем git, т.к. он нужен для скачивания зависимостей
RUN apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
# Скачиваем зависимости
RUN go mod download

COPY . .
# Собираем статичный билд без CGO
RUN CGO_ENABLED=0 GOOS=linux go build -a -o db-migrator .

# Этап 2: Создание минимального образа для запуска
FROM alpine:latest

# Устанавливаем только необходимые для работы утилиты
RUN apk --no-cache add kubectl postgresql-client openssh-client

WORKDIR /app
# Копируем только скомпилированный бинарник с первого этапа
COPY --from=builder /app/db-migrator .
# Копируем файл с данными
COPY values-statefulsets.yaml .

# Точка входа
ENTRYPOINT [ "./db-migrator" ]
