using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class RPCClient
{
    private IConnection _connection;
    private IModel _channel;
    private string replyQueueName;
    private QueueingBasicConsumer _consumer;

    public RPCClient()
    {
        var factory = new ConnectionFactory() { HostName = "172.17.0.2", UserName = "guest", Password = "guest" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.QueueDeclare("rpc_test_queue", false, false, false, null);
        _channel.BasicQos(0, 1, false);
        replyQueueName = _channel.QueueDeclare().QueueName;
        _consumer = new QueueingBasicConsumer(_channel);
        _channel.BasicConsume(queue: replyQueueName, autoAck: false, consumer: _consumer);
    }

    public string Call(string message)
    {
        var corrId = Guid.NewGuid().ToString();
        var props = _channel.CreateBasicProperties();
        props.ReplyTo = replyQueueName;
        props.CorrelationId = corrId;

        var messageBytes = Encoding.UTF8.GetBytes(message);
        _channel.BasicPublish(exchange: "", routingKey: "rpc_test_queue", basicProperties: props, body: messageBytes);

        while (true)
        {
            var ea = (BasicDeliverEventArgs)_consumer.Queue.Dequeue();
            if (ea.BasicProperties.CorrelationId == corrId)
            {
                return Encoding.UTF8.GetString(ea.Body);
            }
        }
    }

    public void Close()
    {
        _connection.Close();
    }
}

// class RPC
// {
//     public static void Main()
//     {
//         var rpcClient = new RPCClient();

//         var n = args.Length > 0 ? args[0] : "30";
//         Console.WriteLine(" [x] Requesting fib({0})", n);
//         var response = rpcClient.Call(n);
//         Console.WriteLine(" [.] Got '{0}'", response);

//         rpcClient.Close();

//         Console.WriteLine(" Press [enter] to exit.");
//         Console.ReadLine();
//     }
// }