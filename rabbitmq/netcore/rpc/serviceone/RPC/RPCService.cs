using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

class RPCService
{

    // private IConnection _connectionService;
    // private IModel _channelService;
    // private string replyQueueName;
    // private EventingBasicConsumer _consumerService;

    // public RPCService()
    // {
    //     var factory1 = new ConnectionFactory() { HostName = "172.17.0.2", UserName = "guest", Password = "guest" };
    //     _connectionService = factory1.CreateConnection();
    //     _channelService = _connectionService.CreateModel();
    //     _channelService.QueueDeclare(queue: "rpc_test_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
    //     _channelService.BasicQos(0, 1, false);

    //     //replyQueueName = _channelService.QueueDeclare().QueueName;
    //     _consumerService = new EventingBasicConsumer(_channelService);
    //     //_channelService.BasicConsume(queue: replyQueueName, autoAck: false, consumer: _consumerService);
    // }

    public void Listen()
    {
        Console.WriteLine("listener started");
        var factory = new ConnectionFactory()
        {
            HostName = "172.17.0.2",
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
            AutomaticRecoveryEnabled = true,
            UserName = "guest",
            Password = "guest"
        };

        var connection = factory.CreateConnection();
        var channel = connection.CreateModel();
        channel.QueueDeclare(queue: "rpc_test_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
        var consumer = new EventingBasicConsumer(channel);

        consumer.Received += (model, ea) =>
                    {
                        Console.WriteLine("listener beep");
                        string response = null;

                        var body = ea.Body;
                        var props = ea.BasicProperties;
                        var replyProps = channel.CreateBasicProperties();
                        replyProps.CorrelationId = props.CorrelationId;

                        try
                        {
                            var message = Encoding.UTF8.GetString(body);
                            int n = int.Parse(message);
                            Console.WriteLine(" [.] listener fib({0})", message);
                            response = fib(n).ToString();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(" [.] " + e.Message);
                            response = "";
                        }
                        finally
                        {
                            var responseBytes = Encoding.UTF8.GetBytes(response);
                            channel.BasicPublish(exchange: "", routingKey: props.ReplyTo, basicProperties: replyProps, body: responseBytes);
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                    };

        channel.BasicConsume(queue: "rpc_test_queue", autoAck: false, consumer: consumer);
    }

    private static int fib(int n)
    {
        if (n == 0 || n == 1)
        {
            return n;
        }

        return fib(n - 1) + fib(n - 2);
    }
}