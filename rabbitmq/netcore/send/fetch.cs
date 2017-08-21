using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace send
{
    class Fetch
    {
        public void Run()
        {
            Console.WriteLine("Fetching message");
            // Hostname is the ipaddress of the server, if in Docker, needs to be the
            // IPAddress of the container
            // Attempt recovery every 10 seconds if network connection is lost
            var factory = new ConnectionFactory()
            {
                HostName = "172.17.0.2",
                NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
                AutomaticRecoveryEnabled = true
            };
            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();
            channel.QueueDeclare(queue: "hello", durable: false, exclusive: false, autoDelete: false, arguments: null);
            var consumer = new EventingBasicConsumer(channel);

            // sets up a listener that runs for the life of the application
            consumer.Received += (model, args) =>
                        {
                            // tells the channel that it was received for processing
                            channel.BasicAck(deliveryTag: args.DeliveryTag, multiple: false);

                            var body = args.Body;
                            var message = Encoding.UTF8.GetString(body);
                            Console.WriteLine(" [-] Received {0}", message);
                        };

            channel.BasicConsume(queue: "hello", autoAck: false, consumer: consumer);
        }
    }
}
