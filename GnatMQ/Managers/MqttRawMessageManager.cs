namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;

    using GnatMQForAzure.Entities;

    public class MqttRawMessageManager
    {
        private readonly ConcurrentStack<MqttRawMessage> rawMessageBuffer;

        private readonly int individualMessageBufferSize;

        public MqttRawMessageManager(int initialBufferSize, int individualMessageBufferSize)
        {
            rawMessageBuffer = new ConcurrentStack<MqttRawMessage>();
            this.individualMessageBufferSize = individualMessageBufferSize;

            for (int i = 0; i < initialBufferSize; i++)
            {
                rawMessageBuffer.Push(new MqttRawMessage(individualMessageBufferSize));
            }
        }

        public MqttRawMessage GetRawMessageWithData(byte messageType, byte[] buffer, int bufferOffset, int payloadLength)
        {
            MqttRawMessage rawMessage;
            if (!rawMessageBuffer.TryPop(out rawMessage))
            {
                rawMessage = new MqttRawMessage(individualMessageBufferSize);
            }

            rawMessage.MessageType = messageType;
            rawMessage.PayloadLength = payloadLength;
            Buffer.BlockCopy(buffer, bufferOffset, rawMessage.PayloadBuffer, 0, payloadLength);
            
            return rawMessage;
        }

        public void ReturnRawMessageToBuffer(MqttRawMessage message)
        {
            rawMessageBuffer.Push(message);
        }
    }
}