using KafkaProducer.Domain;
using KafkaProducer.Service;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace KafkaProducer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : Controller
    {
        private readonly IKafkaProducerService _kafkaProducerService;
        private const string TOPIC = "ecommerce_new_order";

        public ProducerController(IKafkaProducerService kafkaProducerService)
        {
            _kafkaProducerService = kafkaProducerService;
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] OrderRequest orderRequest)
        {
            string message = JsonSerializer.Serialize(orderRequest);
            return Ok(await _kafkaProducerService.SendMessage(TOPIC, message));
        }
    }
}
