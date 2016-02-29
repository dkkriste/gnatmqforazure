namespace GnatMQForAzure.Managers
{
    using System;
    using System.IO;
    using System.Net.Sockets;

    using GnatMQForAzure.Events;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Utility;

    public class MqttClientConnectionReceiveManager
    {
        /// <summary>
        /// Thread for receiving messages
        /// </summary>
        private void ReceiveThread(MqttClientConnection clientConnection)
        {
            int readBytes = 0;
            byte[] fixedHeaderFirstByte = new byte[1];
            byte msgType;

            while (clientConnection.isRunning)
            {
                try
                {
                    // read first byte (fixed header)
                    readBytes = clientConnection.channel.Receive(fixedHeaderFirstByte);

                    if (readBytes > 0)
                    {
                        // update last message received ticks
                        clientConnection.lastCommTime = Environment.TickCount;

                        // extract message type from received byte
                        msgType = (byte)((fixedHeaderFirstByte[0] & MqttMsgBase.MSG_TYPE_MASK) >> MqttMsgBase.MSG_TYPE_OFFSET);

                        switch (msgType)
                        {
                            // CONNECT message received
                            case MqttMsgBase.MQTT_MSG_CONNECT_TYPE:

                                MqttMsgConnect connect = MqttMsgConnect.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);
                                // raise message received event
                                clientConnection.OnInternalEvent(new MsgInternalEvent(connect));
                                break;

                            // CONNACK message received
                            case MqttMsgBase.MQTT_MSG_CONNACK_TYPE:
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);

                            // PINGREQ message received
                            case MqttMsgBase.MQTT_MSG_PINGREQ_TYPE:

                                clientConnection.msgReceived = MqttMsgPingReq.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);

                                MqttMsgPingResp pingresp = new MqttMsgPingResp();
                                clientConnection.Send(pingresp);

                                break;


                            // PINGRESP message received
                            case MqttMsgBase.MQTT_MSG_PINGRESP_TYPE:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);


                            // SUBSCRIBE message received
                            case MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE:

                                MqttMsgSubscribe subscribe = MqttMsgSubscribe.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);


                                // raise message received event
                                clientConnection.OnInternalEvent(new MsgInternalEvent(subscribe));

                                break;


                            // SUBACK message received
                            case MqttMsgBase.MQTT_MSG_SUBACK_TYPE:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);


                            // PUBLISH message received
                            case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:

                                MqttMsgPublish publish = MqttMsgPublish.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);


                                // enqueue PUBLISH message to acknowledge into the inflight queue
                                EnqueueInflight(clientConnection, publish, MqttMsgFlow.ToAcknowledge);

                                break;

                            // PUBACK message received
                            case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:

                                // enqueue PUBACK message received (for QoS Level 1) into the internal queue
                                MqttMsgPuback puback = MqttMsgPuback.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);
                                // enqueue PUBACK message into the internal queue
                                EnqueueInternal(clientConnection, puback);

                                break;

                            // PUBREC message received
                            case MqttMsgBase.MQTT_MSG_PUBREC_TYPE:

                                // enqueue PUBREC message received (for QoS Level 2) into the internal queue
                                MqttMsgPubrec pubrec = MqttMsgPubrec.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);
                                // enqueue PUBREC message into the internal queue
                                EnqueueInternal(clientConnection, pubrec);

                                break;

                            // PUBREL message received
                            case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:

                                // enqueue PUBREL message received (for QoS Level 2) into the internal queue
                                MqttMsgPubrel pubrel = MqttMsgPubrel.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);

                                // enqueue PUBREL message into the internal queue
                                EnqueueInternal(clientConnection, pubrel);

                                break;

                            // PUBCOMP message received
                            case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:

                                // enqueue PUBCOMP message received (for QoS Level 2) into the internal queue
                                MqttMsgPubcomp pubcomp = MqttMsgPubcomp.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);

                                // enqueue PUBCOMP message into the internal queue
                                EnqueueInternal(clientConnection, pubcomp);

                                break;

                            // UNSUBSCRIBE message received
                            case MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE:

                                MqttMsgUnsubscribe unsubscribe = MqttMsgUnsubscribe.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);

                                // raise message received event
                                clientConnection.OnInternalEvent(new MsgInternalEvent(unsubscribe));

                                break;


                            // UNSUBACK message received
                            case MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE:
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);


                            // DISCONNECT message received
                            case MqttMsgDisconnect.MQTT_MSG_DISCONNECT_TYPE:

                                MqttMsgDisconnect disconnect = MqttMsgDisconnect.Parse(fixedHeaderFirstByte[0], (byte)clientConnection.ProtocolVersion, clientConnection.channel);


                                // raise message received event
                                clientConnection.OnInternalEvent(new MsgInternalEvent(disconnect));

                                break;

                            default:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
                        }

                        clientConnection.exReceiving = null;
                    }
                    // zero bytes read, peer gracefully closed socket
                    else
                    {
                        // wake up thread that will notify connection is closing
                        clientConnection.OnConnectionClosing();
                    }
                }
                catch (Exception e)
                {
                    clientConnection.exReceiving = new MqttCommunicationException(e);

                    bool close = false;
                    if (e.GetType() == typeof(MqttClientException))
                    {
                        // [v3.1.1] scenarios the receiver MUST close the network connection
                        MqttClientException ex = e as MqttClientException;
                        close = ((ex.ErrorCode == MqttClientErrorCode.InvalidFlagBits) ||
                                (ex.ErrorCode == MqttClientErrorCode.InvalidProtocolName) ||
                                (ex.ErrorCode == MqttClientErrorCode.InvalidConnectFlags));
                    }
                    else if ((e.GetType() == typeof(IOException)) || (e.GetType() == typeof(SocketException)) ||
                             ((e.InnerException != null) && (e.InnerException.GetType() == typeof(SocketException)))) // added for SSL/TLS incoming connection that use SslStream that wraps SocketException
                    {
                        close = true;
                    }

                    if (close)
                    {
                        // wake up thread that will notify connection is closing
                        clientConnection.OnConnectionClosing();
                    }
                }
            }
        }

        /// <summary>
        /// Enqueue a message into the inflight queue
        /// </summary>
        /// <param name="msg">Message to enqueue</param>
        /// <param name="flow">Message flow (publish, acknowledge)</param>
        /// <returns>Message enqueued or not</returns>
        private bool EnqueueInflight(MqttClientConnection clientConnection, MqttMsgBase msg, MqttMsgFlow flow)
        {
            // enqueue is needed (or not)
            bool enqueue = true;

            // if it is a PUBLISH message with QoS Level 2
            if ((msg.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) &&
                (msg.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE))
            {
                lock (clientConnection.inflightQueue)
                {
                    // if it is a PUBLISH message already received (it is in the inflight queue), the publisher
                    // re-sent it because it didn't received the PUBREC. In clientConnection case, we have to re-send PUBREC

                    // NOTE : I need to find on message id and flow because the broker could be publish/received
                    //        to/from client and message id could be the same (one tracked by broker and the other by client)
                    MqttClientConnection.MqttMsgContextFinder msgCtxFinder = new MqttClientConnection.MqttMsgContextFinder(msg.MessageId, MqttMsgFlow.ToAcknowledge);
                    MqttMsgContext msgCtx = (MqttMsgContext)clientConnection.inflightQueue.Get(msgCtxFinder.Find);

                    // the PUBLISH message is alredy in the inflight queue, we don't need to re-enqueue but we need
                    // to change state to re-send PUBREC
                    if (msgCtx != null)
                    {
                        msgCtx.State = MqttMsgState.QueuedQos2;
                        msgCtx.Flow = MqttMsgFlow.ToAcknowledge;
                        enqueue = false;
                    }
                }
            }

            if (enqueue)
            {
                // set a default state
                MqttMsgState state = MqttMsgState.QueuedQos0;

                // based on QoS level, the messages flow between broker and client changes
                switch (msg.QosLevel)
                {
                    // QoS Level 0
                    case MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE:

                        state = MqttMsgState.QueuedQos0;
                        break;

                    // QoS Level 1
                    case MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE:

                        state = MqttMsgState.QueuedQos1;
                        break;

                    // QoS Level 2
                    case MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE:

                        state = MqttMsgState.QueuedQos2;
                        break;
                }

                // [v3.1.1] SUBSCRIBE and UNSUBSCRIBE aren't "officially" QOS = 1
                //          so QueuedQos1 state isn't valid for them
                if (msg.Type == MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE)
                    state = MqttMsgState.SendSubscribe;
                else if (msg.Type == MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE)
                    state = MqttMsgState.SendUnsubscribe;

                // queue message context
                MqttMsgContext msgContext = new MqttMsgContext()
                {
                    Message = msg,
                    State = state,
                    Flow = flow,
                    Attempt = 0
                };

                lock (clientConnection.inflightQueue)
                {
                    // check number of messages inside inflight queue 
                    enqueue = (clientConnection.inflightQueue.Count < clientConnection.Settings.InflightQueueSize);

                    if (enqueue)
                    {
                        // enqueue message and unlock send thread
                        clientConnection.inflightQueue.Enqueue(msgContext);

                        // PUBLISH message
                        if (msg.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                        {
                            // to publish and QoS level 1 or 2
                            if ((msgContext.Flow == MqttMsgFlow.ToPublish) &&
                                ((msg.QosLevel == MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE) ||
                                 (msg.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE)))
                            {
                                if (clientConnection.Session != null)
                                    clientConnection.Session.InflightMessages.Add(msgContext.Key, msgContext);
                            }
                            // to acknowledge and QoS level 2
                            else if ((msgContext.Flow == MqttMsgFlow.ToAcknowledge) &&
                                     (msg.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE))
                            {
                                if (clientConnection.Session != null)
                                    clientConnection.Session.InflightMessages.Add(msgContext.Key, msgContext);
                            }
                        }
                    }
                }
            }

            clientConnection.inflightWaitHandle.Set();

            return enqueue;
        }

        /// <summary>
        /// Enqueue a message into the internal queue
        /// </summary>
        /// <param name="msg">Message to enqueue</param>
        private void EnqueueInternal(MqttClientConnection clientConnection, MqttMsgBase msg)
        {
            // enqueue is needed (or not)
            bool enqueue = true;

            // if it is a PUBREL message (for QoS Level 2)
            if (msg.Type == MqttMsgBase.MQTT_MSG_PUBREL_TYPE)
            {
                lock (clientConnection.inflightQueue)
                {
                    // if it is a PUBREL but the corresponding PUBLISH isn't in the inflight queue,
                    // it means that we processed PUBLISH message and received PUBREL and we sent PUBCOMP
                    // but publisher didn't receive PUBCOMP so it re-sent PUBREL. We need only to re-send PUBCOMP.

                    // NOTE : I need to find on message id and flow because the broker could be publish/received
                    //        to/from client and message id could be the same (one tracked by broker and the other by client)
                    MqttClientConnection.MqttMsgContextFinder msgCtxFinder = new MqttClientConnection.MqttMsgContextFinder(msg.MessageId, MqttMsgFlow.ToAcknowledge);
                    MqttMsgContext msgCtx = (MqttMsgContext)clientConnection.inflightQueue.Get(msgCtxFinder.Find);

                    // the PUBLISH message isn't in the inflight queue, it was already processed so
                    // we need to re-send PUBCOMP only
                    if (msgCtx == null)
                    {
                        MqttMsgPubcomp pubcomp = new MqttMsgPubcomp();
                        pubcomp.MessageId = msg.MessageId;

                        clientConnection.Send(pubcomp);

                        enqueue = false;
                    }
                }
            }
            // if it is a PUBCOMP message (for QoS Level 2)
            else if (msg.Type == MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE)
            {
                lock (clientConnection.inflightQueue)
                {
                    // if it is a PUBCOMP but the corresponding PUBLISH isn't in the inflight queue,
                    // it means that we sent PUBLISH message, sent PUBREL (after receiving PUBREC) and already received PUBCOMP
                    // but publisher didn't receive PUBREL so it re-sent PUBCOMP. We need only to ignore clientConnection PUBCOMP.

                    // NOTE : I need to find on message id and flow because the broker could be publish/received
                    //        to/from client and message id could be the same (one tracked by broker and the other by client)
                    MqttClientConnection.MqttMsgContextFinder msgCtxFinder = new MqttClientConnection.MqttMsgContextFinder(msg.MessageId, MqttMsgFlow.ToPublish);
                    MqttMsgContext msgCtx = (MqttMsgContext)clientConnection.inflightQueue.Get(msgCtxFinder.Find);

                    // the PUBLISH message isn't in the inflight queue, it was already sent so we need to ignore clientConnection PUBCOMP
                    if (msgCtx == null)
                    {
                        enqueue = false;
                    }
                }
            }
            // if it is a PUBREC message (for QoS Level 2)
            else if (msg.Type == MqttMsgBase.MQTT_MSG_PUBREC_TYPE)
            {
                lock (clientConnection.inflightQueue)
                {
                    // if it is a PUBREC but the corresponding PUBLISH isn't in the inflight queue,
                    // it means that we sent PUBLISH message more times (retries) but broker didn't send PUBREC in time
                    // the publish is failed and we need only to ignore clientConnection PUBREC.

                    // NOTE : I need to find on message id and flow because the broker could be publish/received
                    //        to/from client and message id could be the same (one tracked by broker and the other by client)
                    MqttClientConnection.MqttMsgContextFinder msgCtxFinder = new MqttClientConnection.MqttMsgContextFinder(msg.MessageId, MqttMsgFlow.ToPublish);
                    MqttMsgContext msgCtx = (MqttMsgContext)clientConnection.inflightQueue.Get(msgCtxFinder.Find);

                    // the PUBLISH message isn't in the inflight queue, it was already sent so we need to ignore clientConnection PUBREC
                    if (msgCtx == null)
                    {
                        enqueue = false;
                    }
                }
            }

            if (enqueue)
            {
                lock (clientConnection.internalQueue)
                {
                    clientConnection.internalQueue.Enqueue(msg);
                    clientConnection.inflightWaitHandle.Set();
                }
            }
        }
    }
}