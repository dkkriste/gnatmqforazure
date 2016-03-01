namespace GnatMQForAzure.Entities
{
    public class MqttRawMessage
    {
        public readonly byte[] PayloadBuffer;

        public MqttRawMessage(int messageBufferSize)
        {
            PayloadBuffer = new byte[messageBufferSize];
        }

        public byte MessageType { get; set; }

        public int PayloadLength { get; set; }
    }
}