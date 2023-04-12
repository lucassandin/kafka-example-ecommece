using Confluent.Kafka;

namespace LogService.Service
{
    public interface ILogConsumeService
    {
        Task Parse(ConsumeResult<string, string> events);

        Task StartAsync(CancellationToken cancellationToken);

        Task StopAsync(CancellationToken cancellationToken);
    }
}
