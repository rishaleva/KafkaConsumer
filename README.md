## Инструкция:
1. Клонируем kafkaProducer и kafkaConsumer.
2. docker-compose up для запуска контейнера Kafka
3. Успешно Запускаем проекты
4. Открываем POSTMAN, выполняем POST http://localhost:8080/send с BODY Json: 
```
{
    "name": "someName",
    "position": "somepositionInIT",
    "salary": someSalaryInInteger
}
```
4. Смотрим консоль kafkaConsumer
