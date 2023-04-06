using Confluent.Kafka;
using System.Diagnostics;
using System.Net;

namespace KafkaProducer.Service
{
    public class KafkaProducerService : IKafkaProducerService
    {
        private const string BOOTSTRAP_SERVER = "localhost:9092";
        
        public async Task<bool> SendMessage(string topic, string message)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = BOOTSTRAP_SERVER,
                ClientId = Dns.GetHostName(),
            };

            try
            {
                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var result = await producer.ProduceAsync(topic, new Message<Null, string>
                    {
                        Value = message
                    });

                    Debug.WriteLine($"Delivery timestamp: {result.Timestamp.UtcDateTime}");
                }

                return await Task.FromResult(true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
            }

            return await Task.FromResult(false);
        }
    }
}
