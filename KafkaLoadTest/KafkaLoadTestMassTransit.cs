using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace KafkaLoadTest;

public class KafkaLoadTestMassTransit
{
    private readonly ITestOutputHelper _testOutputHelper;

    public KafkaLoadTestMassTransit(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    private const string TopicName = "masstransit-topic";
    private const int MessageCount = 1_000_000;
    private static readonly string BootstrapServer = "localhost:9092";

    [Fact]
    public async Task LoadTest()
    {
        var services = new ServiceCollection();

        // Регистрация MassTransit с Kafka <button class="citation-flag" data-index="6">
        services.AddMassTransit(x =>
        {
            x.UsingInMemory(); // Для внутренних нужд MassTransit
            x.AddRider(rider =>
            {
                rider.AddConsumer<MessageConsumer>();
                // Добавляем продюсера для сообщения MyMessage с настройкой топика:
                rider.AddProducer<KafkaMessagePC>(
                    TopicName, new ProducerConfig
                    {
                        BootstrapServers = BootstrapServer,
                        Acks = Acks.All,
                        BatchSize = 16384,
                        LingerMs = 5,
                        CompressionType = CompressionType.Snappy,
                        EnableIdempotence = true
                    }); // Регистрация продюсера
                rider.UsingKafka((context, k) =>
                {
                    k.Host(BootstrapServer);
                    k.TopicEndpoint<KafkaMessagePC>(TopicName, "mt-group", e =>
                    {
                        e.ConfigureConsumer(context, typeof(MessageConsumer)); // Для проверки доставки
                        e.CreateIfMissing(options =>
                        {
                            options.NumPartitions = 1;
                            options.ReplicationFactor = 1;
                            options.RequestTimeout = TimeSpan.FromSeconds(10);
                        });
                    });
                });
            });
        });

        await using (var provider = services.BuildServiceProvider())
        {
            var busControl = provider.GetRequiredService<IBusControl>();
            await busControl.StartAsync(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);

            var producer = provider.GetRequiredService<ITopicProducer<KafkaMessagePC>>();

            // Настройка ActionBlock для параллелизма <button class="citation-flag" data-index="1"><button class="citation-flag" data-index="4">
            var block = new ActionBlock<int>(async i =>
            {
                try
                {
                    await producer.Produce(new KafkaMessagePC($"Message {i}"));
                }
                catch (Exception ex)
                {
                    _testOutputHelper.WriteLine(
                        $"Ошибка: {ex.Message}"); // Обработка ошибок <button class="citation-flag" data-index="4">
                }
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 2048,
                //BoundedCapacity = 10_000
            });

            _testOutputHelper.WriteLine($"{DateTime.Now.ToString("O")} Начинаем отправку {MessageCount} сообщений");
            // Запуск теста
            var stopwatch = Stopwatch.StartNew();

            for (int i = 0; i < MessageCount; i++)
            {
                block.Post(i); // Асинхронная отправка <button class="citation-flag" data-index="7">
            }

            block.Complete();
            await block.Completion;

            _testOutputHelper.WriteLine($"{DateTime.Now.ToString("O")} Отправка завершена, останавливаем шину");
            await busControl.StopAsync();
            stopwatch.Stop();

            _testOutputHelper.WriteLine(
                $"{DateTime.Now.ToString("O")} Отправка {MessageCount} сообщений завершена через {stopwatch.Elapsed}");
            _testOutputHelper.WriteLine(
                $"{DateTime.Now.ToString("O")} Производительность: {MessageCount / stopwatch.Elapsed.TotalSeconds:F2} msg/s");
        }
    }
}