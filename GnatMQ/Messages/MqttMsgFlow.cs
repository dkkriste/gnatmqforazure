namespace GnatMQForAzure.Messages
{
    /// <summary>
    /// Flow of the message
    /// </summary>
    public enum MqttMsgFlow
    {
        /// <summary>
        /// To publish to subscribers
        /// </summary>
        ToPublish,

        /// <summary>
        /// To acknowledge to publisher
        /// </summary>
        ToAcknowledge
    }
}