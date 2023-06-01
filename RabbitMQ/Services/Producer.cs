using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace RabbitMQ.Services
{
    public class Producer
    {
        private int _messageCount = 1;
        private readonly IMemoryCache _memoryCache;

        public Producer(IMemoryCache memoryCache)
        {
            _memoryCache = memoryCache;
        }

        public bool PushMessageToQ()
        {
            try
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                using (var connection = factory.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        // declare exchange
                        channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);
                        channel.QueueDeclare(queue: "counter",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                        var message = $"Message {_messageCount++}";

                        var messageBody = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish(exchange: "logs", routingKey: "", body: messageBody, basicProperties: null);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message} | {ex.StackTrace}");
                return false;
            }
        }
    }
}
