namespace GnatMQForAzure.Entities
{
    using System.Net;

    public class MqttOptions
    {
        public int NumberOfAcceptSaea { get; set; }

        public int MaxConnections { get; set; }

        public int ConnectionsPrProcessingManager { get; set; }

        public IPEndPoint EndPoint { get; set; }

        public int InitialNumberOfRawMessages { get; set; }

        public int IndividualMessageBufferSize { get; set; }

        public int NumberOfSendBuffers { get; set; }

        public int ReadAndSendBufferSize { get; set; }
    }
}