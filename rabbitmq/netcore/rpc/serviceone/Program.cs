using System;
using System.Threading.Tasks;

namespace serviceone
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var t = Task.Run(() => rpcCall());
            var t2 = Task.Run(() => listenCall());
            t.Wait();
            t2.Wait();


        }
        static void rpcCall()
        {
            var rpcClient = new RPCClient();

            var n = "30";
            Console.WriteLine(" [x] Requesting fib({0})", n);
            var response = rpcClient.Call(n);
            Console.WriteLine(" [.] Got '{0}'", response);

            rpcClient.Close();

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        static void listenCall()
        {
            var rpcListener = new RPCService();
            rpcListener.Listen();
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

    }


}