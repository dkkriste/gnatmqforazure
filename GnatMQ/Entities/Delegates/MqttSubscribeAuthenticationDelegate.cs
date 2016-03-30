namespace GnatMQForAzure.Entities.Delegates
{
    public delegate bool MqttSubscribeAuthenticationDelegate(MqttClientConnection clientConnection, string topic);
}