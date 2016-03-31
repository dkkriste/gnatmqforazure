namespace GnatMQForAzure.Utility
{
    public class LoggerConstants
    {
        #region MqttBroker

        public const string NumberOfConnectedClients = "NumberOfConnectedClients";

        #endregion

        #region MqttClientConnectionProcessingManager

        public const string RawMessageQueueSize = "RawMessageQueueSize";

        public const string InflightQueuesToProcessSize = "InflightQueuesToProcessSize";

        public const string EventQueuesToProcessSize = "EventQueuesToProcessSize";

        public const string NumberOfRawMessagsProcessed = "NumberOfRawMessagsProcessed";

        public const string NumberOfInflightQueuesProcessed = "NumberOfInflightQueuesProcessed";

        public const string NumberOfInternalEventQueuesProcessed = "NumberOfEventQueuesProcessed";

        #endregion

        #region PublishManager

        public const string PublishQueueSize = "PublishQueueSize";

        public const string NumberOfMessagesPublished = "NumberOfMessagesPublished";

        #endregion

        #region Loadbalancer

        public const string NumberOfConnectionsLoadbalanced = "NumberOfConnectionsLoadbalanced";

        #endregion
    }
}