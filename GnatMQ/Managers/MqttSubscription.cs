namespace GnatMQForAzure.Managers
{
    /// <summary>
    /// MQTT subscription
    /// </summary>
    public class MqttSubscription
    {
        /// <summary>
        /// Client Id
        /// </summary>
        public string ClientId { get; set; }

        /// <summary>
        /// Topic of subscription
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// QoS level granted for the subscription
        /// </summary>
        public byte QosLevel { get; set; }

        /// <summary>
        /// Client related to the subscription
        /// </summary>
        public MqttClientConnection ClientConnection { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public MqttSubscription()
        {
            this.ClientId = null;
            this.Topic = null;
            this.QosLevel = 0;
            this.ClientConnection = null;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="clientId">Client Id of the subscription</param>
        /// <param name="topic">Topic of subscription</param>
        /// <param name="qosLevel">QoS level of subscription</param>
        /// <param name="clientConnection">Client related to the subscription</param>
        public MqttSubscription(string clientId, string topic, byte qosLevel, MqttClientConnection clientConnection = null)
        {
            this.ClientId = clientId;
            this.Topic = topic;
            this.QosLevel = qosLevel;
            this.ClientConnection = clientConnection;
        }

        /// <summary>
        /// Dispose subscription
        /// </summary>
        public void Dispose()
        {
            this.ClientId = null;
            this.Topic = null;
            this.QosLevel = 0;
            this.ClientConnection = null;
        }
    }
}