using MassTransit;

namespace KafkaLoadTest;

public class MessageConsumer : IConsumer<KafkaMessage>
{
    public Task Consume(ConsumeContext<KafkaMessage> context)
    {
        // Здесь можно добавить логику проверки
        return Task.CompletedTask;
    }
}