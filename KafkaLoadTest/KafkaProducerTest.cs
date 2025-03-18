using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace KafkaLoadTest;

[Collection("KafkaLoadTest")]
public class KafkaProducerTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public KafkaProducerTest(ITestOutputHelper testOutputHelper)
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

        // Создаем ActionBlock с настройками параллелизма
        var options = new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = 2048, // Количество параллельных задач
            //BoundedCapacity = 10_000    // Ограничение очереди
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            var actionBlock = new ActionBlock<int>(async i =>
            {
                try
                {
                    await producer.ProduceAsync(Topic, new Message<Null, string> { Value = $"Message {i}" });
                }
                catch (ProduceException<Null, string> ex)
                {
                    _testOutputHelper.WriteLine(
                        $"Ошибка отправки: {ex.Error.Reason}"); // <button class="citation-flag" data-index="4">
                }
            }, options);

            // Запуск замера времени
            var stopwatch = Stopwatch.StartNew();
            _testOutputHelper.WriteLine($"{DateTime.Now.ToString("O")} Запуск отправки {MessageCount} сообщений");

            // Отправляем сообщения в блок
            for (int i = 0; i < MessageCount; i++)
            {
                actionBlock.Post(i); // Асинхронная отправка в очередь блока
            }

            // Завершаем работу блока
            actionBlock.Complete();
            await actionBlock.Completion;

            stopwatch.Stop();
            _testOutputHelper.WriteLine(
                $"{DateTime.Now.ToString("O")} Отправка {MessageCount} сообщений завершена через {stopwatch.Elapsed}");
            _testOutputHelper.WriteLine(
                $"Производительность: {MessageCount / stopwatch.Elapsed.TotalSeconds:F2} msg/s");
        }
    }
}