namespace GnatMQForAzure.Entities.Delegates
{
    public delegate bool MqttPublishAuthenticationDelegate(MqttClientConnection clientConnection, string topic);
}