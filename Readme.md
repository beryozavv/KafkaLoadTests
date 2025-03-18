# Результаты тестов 
> Для сравнения потребления памяти необходимо каждый тест запускать в отдельном процессе и по очереди  
> Уровень параллелизма и количество сообщений в каждом примере настроены согласно конфигурации тестовой машины (16 виртуальных ядер и 32Гб ОЗУ)
## KafkaLoadTest
### KafkaProducerTest
```shell
2025-03-18T13:19:00.4424816+02:00 Запуск отправки 3000000 сообщений
2025-03-18T13:19:08.8624627+02:00 Отправка 3000000 сообщений завершена через 00:00:08.4199817
Производительность: 356295,31 msg/s
```
### KafkaProducerContinueWithTest
```shell
2025-03-18T13:21:46.7608975+02:00 Отправка 3000000 сообщений завершена через 00:00:09.2075915
Производительность: 325818,10 msg/s
```