namespace GnatMQForAzure.Contracts
{
    public interface IMqttClientConnectionManager : IPeriodicallyLoggable
    {
        MqttClientConnection GetConnection();

        void ReturnConnection(MqttClientConnection clientConnection);
    }
}