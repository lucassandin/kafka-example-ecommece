namespace KafkaProducer.Service
{
    public interface IKafkaProducerService
    {
        Task<bool> SendMessage(string topic, string message);
    }
}
