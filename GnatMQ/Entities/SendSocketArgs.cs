namespace GnatMQForAzure.Entities
{
    public class SendSocketArgs
    {
        public readonly int BufferOffset;

        public readonly int BufferSize;

        public SendSocketArgs(int bufferOffset, int bufferSize)
        {
            this.BufferOffset = bufferOffset;
            this.BufferSize = bufferSize;
        }

        public int MessageLength { get; set; }

        public int MessageStartFromOffset { get; set; }
    }
}