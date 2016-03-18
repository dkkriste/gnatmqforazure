using System;

namespace GnatMQServer
{
    using System.Net;
    using System.Threading;

    using GnatMQForAzure;
    using GnatMQForAzure.Entities;

    public class Program
    {
        public static void Main(string[] args)
        {
            var options = new MqttOptions
                              {
                                  ConnectionsPrProcessingManager = 1024,
                                  EndPoint = new IPEndPoint(IPAddress.Any, 1883),
                                  IndividualMessageBufferSize = 8192,
                                  NumberOfAcceptSaea = 256,
                                  MaxConnections = 1024,
                                  InitialNumberOfRawMessages = 1024,
                                  NumberOfSendBuffers = 1024,
                                  ReadAndSendBufferSize = 8192
                              };

            var logger = new ConsoleLogger();

            // create and start broker
            MqttBroker broker = new MqttBroker(logger, options);
            broker.Start();

            while (true)
            {
                broker.PeriodicLogging();
                Thread.Sleep(new TimeSpan(0, 1, 0));
            } 

            broker.Stop();
        }
    }
}
