using System.Collections.Concurrent;
using System.Diagnostics;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace KafkaLoadTest;

[Collection("KafkaLoadTest")]
public class KafkaProducerContinueWithTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public KafkaProducerContinueWithTest(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    private static readonly string Topic = "test-topic";
    private static readonly int MessageCount = 3_000_000;
    private static readonly string BootstrapServers = "localhost:9092";

    [Fact]
    public async Task ProducerTest()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            BatchSize = 16384,
            LingerMs = 5,
            CompressionType = CompressionType.Snappy,
            EnableIdempotence = true
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            int degreeOfParallelism = 2048; // Максимальное число параллельно выполняемых задач

            SemaphoreSlim semaphore = new SemaphoreSlim(degreeOfParallelism);
            var tasks = new ConcurrentBag<Task>();

            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < MessageCount; i++)
            {
                // Ожидаем появления свободного слота в пуле
                await semaphore.WaitAsync();

                // Запускаем задачу напрямую без использования Task.Run
                Task processingTask = ProcessTaskAsync(producer, i)
                    .ContinueWith(t =>
                    {
                        // Освобождаем слот, когда работа завершена
                        semaphore.Release();

                        // Дополнительная обработка ошибок (опционально)
                        if (t.IsFaulted)
                        {
                            _testOutputHelper.WriteLine($"Ошибка в задаче {i}: {t.Exception}");
                        }
                    });

                tasks.Add(processingTask);
            }

            // Дожидаемся завершения всех задач
            await Task.WhenAll(tasks);

            _testOutputHelper.WriteLine("Все задачи успешно выполнены.");

            stopwatch.Stop();
            _testOutputHelper.WriteLine(
                $"{DateTime.Now.ToString("O")} Отправка {MessageCount} сообщений завершена через {stopwatch.Elapsed}");
            _testOutputHelper.WriteLine(
                $"Производительность: {MessageCount / stopwatch.Elapsed.TotalSeconds:F2} msg/s");
        }
    }

    private Task ProcessTaskAsync(IProducer<Null, string> producer, int i)
    {
        return producer.ProduceAsync(Topic, new Message<Null, string> { Value = $"Message {i}" });
    }
}