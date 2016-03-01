namespace GnatMQForAzure.Utility
{
    using System;
    using System.Security.Authentication;

    using GnatMQForAzure.Entities.Enums;

    /// <summary>
    /// MQTT SSL utility class
    /// </summary>
    public static class MqttSslUtility
    {
        public static SslProtocols ToSslPlatformEnum(MqttSslProtocols mqttSslProtocol)
        {
            switch (mqttSslProtocol)
            {
                case MqttSslProtocols.None:
                    return SslProtocols.None;
                case MqttSslProtocols.SSLv3:
                    return SslProtocols.Ssl3;
                case MqttSslProtocols.TLSv1_0:
                    return SslProtocols.Tls;
                case MqttSslProtocols.TLSv1_1:
                    return SslProtocols.Tls11;
                case MqttSslProtocols.TLSv1_2:
                    return SslProtocols.Tls12;
                default:
                    throw new ArgumentException("SSL/TLS protocol version not supported");
            }
        }
    }
}