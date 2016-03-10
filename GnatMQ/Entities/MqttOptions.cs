namespace GnatMQForAzure.Entities
{
    using System.Net;

    public class MqttOptions
    {
        public int NumberOfAcceptSaea { get; set; }

        public int MaxConnections { get; set; }

        public int ConnectionsPrProcessingManager { get; set; }

        public IPEndPoint EndPoint { get; set; }
    }
}