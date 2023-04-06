using Confluent.Kafka;
using KafkaConsumer.Const;
using KafkaConsumer.Service;

namespace LogService.Service
{
    public class LogConsumeService : IHostedService
    {
        private readonly IEnumerable<string> TOPICS = new List<string> { "ecommerce_new_order", "ecommerce_send_email" };
        private readonly IDictionary<string, string> configs = new Dictionary<string, string>
        {
            { KafkaConst.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" },
            { KafkaConst.AUTO_OFFSET_RESET_CONFIG, "Earliest" },
            { KafkaConst.GROUP_ID_CONFIG, "log_consumer" },
        };

        private readonly IConsumerService<string> _kafkaConsumerService;

        public LogConsumeService(IConsumerService<string> kafkaConsumerService)
        {
            _kafkaConsumerService = kafkaConsumerService;
        }

        public Task Parse(ConsumeResult<string, string> events)
        {
            Console.WriteLine("---------------------------------------------------");
            Console.WriteLine($"LOG PROCESSER: {events.Message.Value.Trim()}");
            Console.WriteLine("---------------------------------------------------");

            return Task.CompletedTask;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                _kafkaConsumerService.ConsumeMessage(Parse, TOPICS, configs);

                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error Log Server:: {ex}");
                StopAsync(cancellationToken);
            }
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
