using Confluent.Kafka;
using System.Runtime.CompilerServices;

namespace KafkaConsumer.Service
{
    public interface IConsumerService<T> where T : class
    {
        Task ConsumeMessage(Func<ConsumeResult<string, string>, Task> funcParse, IEnumerable<string> topics, IDictionary<string, string> overrideConfigs);
    }
}
