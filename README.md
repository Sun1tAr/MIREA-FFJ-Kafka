# Kafka Learning Project

Учебный Java/Spring-проект по интеграции приложения с Apache Kafka.

## Стек

- Java;
- Spring Boot;
- Apache Kafka;
- Docker Compose;
- Maven.

Репозиторий демонстрирует конфигурацию брокера и обмен сообщениями между
компонентами приложения.

## Запуск

Запустите инфраструктуру:

```bash
docker compose up -d
```

Затем соберите и запустите приложение через Maven:

```bash
./mvnw test
./mvnw spring-boot:run
```

В Windows используйте `mvnw.cmd`. После завершения работы остановите контейнеры:

```bash
docker compose down
```
