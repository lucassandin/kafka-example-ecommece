using Confluent.Kafka;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace KafkaConsumer.Service
{
    public class ConsumerService<T> : IConsumerService<T> where T : class
    {
        public async Task ConsumeMessage(Func<ConsumeResult<string, string>, Task> funcParse, IEnumerable<string> Topics, IDictionary<string, string> overrideConfigs)
        {
            var configs = new ConsumerConfig(overrideConfigs);

            try
            {
                using (var consumerBuilder = new ConsumerBuilder<string, string>(configs).Build())
                {
                    consumerBuilder.Subscribe(Topics);

                    try
                    {
                        while (true)
                        {
                            Thread.Sleep(3000);

                            var cancellation = new CancellationTokenSource();
                            ConsumeResult<string, string> consumerResult = consumerBuilder.Consume(cancellation.Token);

                            await funcParse.Invoke(consumerResult);
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
                Console.WriteLine($"Error:: {ex}");
            }
        }
    }
}
