namespace GnatMQForAzure.Contracts
{
    public interface IMqttClientConnectionManager
    {
        MqttClientConnection GetConnection();

        void ReturnConnection(MqttClientConnection clientConnection);
    }
}