namespace GnatMQForAzure.Managers
{
    using System;

    using GnatMQForAzure.Entities.Enums;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Messages;

    public class MqttOutgoingMessageManager
    {
        /// <summary>
        /// Send CONNACK message to the client (connection accepted or not)
        /// </summary>
        /// <param name="connect">CONNECT message with all client information</param>
        /// <param name="returnCode">Return code for CONNACK message</param>
        /// <param name="clientId">If not null, client id assigned by broker</param>
        /// <param name="sessionPresent">Session present on the broker</param>
        public void Connack(MqttClientConnection clientConnection, MqttMsgConnect connect, byte returnCode, string clientId, bool sessionPresent)
        {
            clientConnection.lastCommTime = Environment.TickCount;

            MqttMsgConnack connack = new MqttMsgConnack();
            connack.ReturnCode = returnCode;
            if (clientConnection.ProtocolVersion == MqttProtocolVersion.Version_3_1_1)
            {
                connack.SessionPresent = sessionPresent;
            }

            // ... send it to the client
            this.Send(clientConnection, connack);

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

                clientConnection.keepAlivePeriod = connect.KeepAlivePeriod * 1000; // convert in ms
                // broker has a tolerance of 1.5 specified keep alive period
                clientConnection.keepAlivePeriod += (clientConnection.keepAlivePeriod / 2);

                clientConnection.isConnectionClosing = false;
                clientConnection.IsConnected = true;
            }
            // connection refused, close TCP/IP channel
            else
            {
                clientConnection.OnConnectionClosed();
            }
        }

        /// <summary>
        /// Send SUBACK message to the client
        /// </summary>
        /// <param name="messageId">Message Id for the SUBSCRIBE message that is being acknowledged</param>
        /// <param name="grantedQosLevels">Granted QoS Levels</param>
        public void Suback(MqttClientConnection clientConnection, ushort messageId, byte[] grantedQosLevels)
        {
            MqttMsgSuback suback = new MqttMsgSuback();
            suback.MessageId = messageId;
            suback.GrantedQoSLevels = grantedQosLevels;

            this.Send(clientConnection, suback);
        }

        /// <summary>
        /// Send UNSUBACK message to the client
        /// </summary>
        /// <param name="messageId">Message Id for the UNSUBSCRIBE message that is being acknowledged</param>
        public void Unsuback(MqttClientConnection clientConnection, ushort messageId)
        {
            MqttMsgUnsuback unsuback = new MqttMsgUnsuback();
            unsuback.MessageId = messageId;

            this.Send(clientConnection, unsuback);
        }

        public void Puback(MqttClientConnection clientConnection, ushort messageId)
        {
            MqttMsgPuback puback = new MqttMsgPuback();
            puback.MessageId = messageId;
            Send(clientConnection, puback);
        }

        public void Pubcomp(MqttClientConnection clientConnection, ushort messageId)
        {
            MqttMsgPubcomp pubcomp = new MqttMsgPubcomp();
            pubcomp.MessageId = messageId;
            Send(clientConnection, pubcomp);
        }

        public void Pubrel(MqttClientConnection clientConnection, ushort messageId, bool duplicate)
        {
            MqttMsgPubrel pubrel = new MqttMsgPubrel();
            pubrel.MessageId = messageId;
            pubrel.DupFlag = duplicate;
            Send(clientConnection, pubrel);
        }

        public void Pubrec(MqttClientConnection clientConnection, ushort messageId)
        {
            MqttMsgPubrec pubrec = new MqttMsgPubrec();
            pubrec.MessageId = messageId;
            Send(clientConnection, pubrec);
        }

        public void PingResp(MqttClientConnection clientConnection)
        {
            MqttMsgPingResp pingresp = new MqttMsgPingResp();
            Send(clientConnection, pingresp);
        }

        /// <summary>
        /// Send a message
        /// </summary>
        /// <param name="msgBytes">Message bytes</param>
        public void Send(MqttClientConnection clientConnection, byte[] msgBytes)
        {
            try
            {
                // send message
                clientConnection.channel.Send(msgBytes);
            }
            catch (Exception e)
            {
                throw new MqttCommunicationException(e);
            }
        }

        /// <summary>
        /// Send a message
        /// </summary>
        /// <param name="msg">Message</param>
        public void Send(MqttClientConnection clientConnection, MqttMsgBase msg)
        {
            this.Send(clientConnection, msg.GetBytes((byte)clientConnection.ProtocolVersion));
        }
    }
}