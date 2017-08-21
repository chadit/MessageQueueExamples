using System;

namespace send
{
    class Program
    {
        static void Main(string[] args)
        {
            new Send().Run();
            new Fetch().Run();
            Console.ReadKey();
        }
    }
}
