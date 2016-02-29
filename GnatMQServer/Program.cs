using System;

namespace GnatMQServer
{
    using GnatMQForAzure;

    class Program
    {
        static void Main(string[] args)
        {
            // create and start broker
            MqttBroker broker = new MqttBroker();
            broker.Start();

            Console.ReadLine();

            broker.Stop();
        }
    }
}
