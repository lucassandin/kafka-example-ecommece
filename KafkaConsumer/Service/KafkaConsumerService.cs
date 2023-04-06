using Confluent.Kafka;
using KafkaConsumer.Const;
using KafkaConsumer.Domain;
using System.Diagnostics;
using System.Text.Json;

namespace KafkaConsumer.Service
{
    public class KafkaConsumerService : IHostedService
    {
        private readonly string topic = "ecommerce_new_order";
        private readonly IDictionary<string, string> configs = new Dictionary<string, string>
        {
            { KafkaConst.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" },
            { KafkaConst.AUTO_OFFSET_RESET_CONFIG, "Earliest" },
            { KafkaConst.GROUP_ID_CONFIG, "log_consumer" },
        };

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig(configs);

            try
            {
                using (var consumerBuilder = new ConsumerBuilder<string, string>(config).Build())
                {
                    consumerBuilder.Subscribe(topic);
                    var cancelToken = new CancellationTokenSource();

                    try
                    {
                        while (true)
                        {
                            Thread.Sleep(3000);

                            ConsumeResult<string, string> consumer = consumerBuilder.Consume(cancelToken.Token);
                            var orderRequest = JsonSerializer.Deserialize<OrderProcessingRequest>(consumer.Message.Value);

                            Console.WriteLine($"Processing order id: {orderRequest?.OrderId}");
                            Console.WriteLine($"--------------------------------------------");
                            Console.WriteLine($"GENERIC CONSUMER PROCESSER: {consumer.Message.Value}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumerBuilder.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
