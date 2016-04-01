namespace GnatMQForAzure.Managers
{
    using System;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Entities.Enums;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Messages;

    public static class MqttOutgoingMessageManager
    {
        public static void Connack(MqttClientConnection clientConnection, MqttMsgConnect connect, byte returnCode, string clientId, bool sessionPresent)
        {
            clientConnection.LastCommunicationTime = Environment.TickCount;

            MqttMsgConnack connack = new MqttMsgConnack();
            connack.ReturnCode = returnCode;
            if (clientConnection.ProtocolVersion == MqttProtocolVersion.Version_3_1_1)
            {
                connack.SessionPresent = sessionPresent;
            }

            // ... send it to the client
            Send(clientConnection, connack);

            // connection accepted, start keep alive thread checking
            if (returnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                // [v3.1.1] if client id isn't null, the CONNECT message has a cliend id with zero bytes length
                //          and broker assigned a unique identifier to the client
                clientConnection.ClientId = (clientId == null) ? connect.ClientId : clientId;
                clientConnection.CleanSession = connect.CleanSession;
                clientConnection.WillFlag = connect.WillFlag;
                clientConnection.WillTopic = connect.WillTopic;
                clientConnection.WillMessage = connect.WillMessage;
                clientConnection.WillQosLevel = connect.WillQosLevel;

                clientConnection.KeepAlivePeriod = connect.KeepAlivePeriod * 1000; // convert in ms
                // broker has a tolerance of 1.5 specified keep alive period
                clientConnection.KeepAlivePeriod += (clientConnection.KeepAlivePeriod / 2);

                clientConnection.IsConnectionClosing = false;
                clientConnection.IsConnected = true;
            }
            // connection refused, close TCP/IP channel
            else
            {
                clientConnection.OnConnectionClosed();
            }
        }

        public static void Suback(MqttClientConnection clientConnection, ushort messageId, byte[] grantedQosLevels)
        {
            MqttMsgSuback suback = new MqttMsgSuback();
            suback.MessageId = messageId;
            suback.GrantedQoSLevels = grantedQosLevels;

            Send(clientConnection, suback);
        }

        public static void Unsuback(MqttClientConnection clientConnection, ushort messageId)
        {
            MqttMsgUnsuback unsuback = new MqttMsgUnsuback();
            unsuback.MessageId = messageId;

            Send(clientConnection, unsuback);
        }

        public static void Puback(MqttClientConnection clientConnection, ushort messageId)
        {
            MqttMsgPuback puback = new MqttMsgPuback();
            puback.MessageId = messageId;
            Send(clientConnection, puback);
        }

        public static void Pubcomp(MqttClientConnection clientConnection, ushort messageId)
        {
            MqttMsgPubcomp pubcomp = new MqttMsgPubcomp();
            pubcomp.MessageId = messageId;
            Send(clientConnection, pubcomp);
        }

        public static void Pubrel(MqttClientConnection clientConnection, ushort messageId, bool duplicate)
        {
            MqttMsgPubrel pubrel = new MqttMsgPubrel();
            pubrel.MessageId = messageId;
            pubrel.DupFlag = duplicate;
            Send(clientConnection, pubrel);
        }

        public static void Pubrec(MqttClientConnection clientConnection, ushort messageId)
        {
            MqttMsgPubrec pubrec = new MqttMsgPubrec();
            pubrec.MessageId = messageId;
            Send(clientConnection, pubrec);
        }

        public static void PingResp(MqttClientConnection clientConnection)
        {
            MqttMsgPingResp pingresp = new MqttMsgPingResp();
            Send(clientConnection, pingresp);
        }

        public static void Send(MqttClientConnection clientConnection, byte[] msgBytes)
        {
            try
            {
                MqttAsyncTcpSender.Send(clientConnection.ReceiveSocketAsyncEventArgs.AcceptSocket, msgBytes);
            }
            catch (Exception)
            {
            }
        }

        public static void Send(MqttClientConnection clientConnection, MqttMsgBase msg)
        {
            Send(clientConnection, msg.GetBytes((byte)clientConnection.ProtocolVersion));
        }
    }
}