namespace GnatMQForAzure.Contracts
{
    public interface IMqttRunnable
    {
        void Start();

        void Stop();
    }
}