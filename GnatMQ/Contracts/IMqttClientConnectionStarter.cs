namespace GnatMQForAzure.Contracts
{
    public interface IMqttClientConnectionStarter
    {
        void OpenClientConnection(MqttClientConnection clientConnection);
    }
}