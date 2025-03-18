using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace KafkaLoadTest;

public class KafkaLoadTestWithMassTransit
{
    private readonly ITestOutputHelper _testOutputHelper;

    public KafkaLoadTestWithMassTransit(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    private const string TopicName = "load-test-topic";
    private const int MessageCount = 1_000_000;
    private static readonly string BootstrapServers = "localhost:9092";

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
                
                rider.AddProducer<KafkaMessage>(
                    TopicName, new ProducerConfig
                    {
                        BootstrapServers = BootstrapServers,
                        Acks = Acks.All,
                        BatchSize = 16384,
                        LingerMs = 5,
                        CompressionType = CompressionType.Snappy,
                        EnableIdempotence = true
                    }); // Регистрация продюсера
                rider.UsingKafka((context, k) =>
                {
                    k.Host("localhost:9092");
                    k.TopicEndpoint<KafkaMessage>(TopicName, "load-test-group", e =>
                    {
                        e.ConfigureConsumer(context, typeof(MessageConsumer)); // Для проверки доставки
                    });
                });
            });
        });

        await using (var provider = services.BuildServiceProvider())
        {
            var busControl = provider.GetRequiredService<IBusControl>();
            await busControl.StartAsync(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);

            var producer = provider.GetRequiredService<ITopicProducer<KafkaMessage>>();

            // Настройка ActionBlock для параллелизма <button class="citation-flag" data-index="1"><button class="citation-flag" data-index="4">
            var block = new ActionBlock<int>(async i =>
            {
                try
                {
                    await producer.Produce(new KafkaMessage($"Message {i}"));
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
                await block.SendAsync(i); // Асинхронная отправка <button class="citation-flag" data-index="7">
            }

            block.Complete();
            await block.Completion;

            _testOutputHelper.WriteLine($"{DateTime.Now.ToString("O")} Отправка завершена, останавливаем шину");
            await busControl.StopAsync();
            stopwatch.Stop();

            _testOutputHelper.WriteLine($"{DateTime.Now.ToString("O")} Отправка {MessageCount} сообщений завершена через {stopwatch.Elapsed}");
            _testOutputHelper.WriteLine(
                $"{DateTime.Now.ToString("O")} Производительность: {MessageCount / stopwatch.Elapsed.TotalSeconds:F2} msg/s");
        }
    }

    // Простой консьюмер для проверки <button class="citation-flag" data-index="3">
    class MessageConsumer : IConsumer<KafkaMessage>
    {
        public Task Consume(ConsumeContext<KafkaMessage> context)
        {
            // Здесь можно добавить логику проверки
            return Task.CompletedTask;
        }
    }

    public class KafkaMessage(string Value);
}