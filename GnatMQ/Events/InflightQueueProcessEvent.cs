namespace GnatMQForAzure.Events
{
    public class InflightQueueProcessEvent
    {
        public MqttClientConnection ClientConnection { get; set; }

        public bool IsCallback { get; set; }

        public int CallbackCreationTime { get; set; }
    }
}