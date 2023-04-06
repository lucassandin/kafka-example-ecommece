using Confluent.Kafka;
using KafkaConsumerService.Domain;
using Microsoft.Extensions.Hosting;
using System.Diagnostics;
using System.Text.Json;

namespace KafkaConsumerService.Service
{
    public class ConsumerService : IHostedService
    {
        private readonly string topic = "ecommerce_new_order";
        private readonly string groupId = "test_group";
        private readonly string bootstrapServe = "localhost:9092";

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServe,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

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
                            var consumer = consumerBuilder.Consume(cancellationToken);
                            var orderRequest = JsonSerializer.Deserialize<OrderProcessingRequest>(consumer.Message.Value);

                            Debug.WriteLine($"Processing order id: {orderRequest?.OrderId}");
                            Debug.WriteLine($"--------------------------------------------");
                            Debug.WriteLine($"{consumer?.Message.Value}");
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
                Debug.WriteLine(ex.Message);
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
