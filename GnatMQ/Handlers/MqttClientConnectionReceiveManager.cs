namespace GnatMQForAzure.Handlers
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net.Sockets;

    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Events;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Managers;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Utility;

    public class MqttClientConnectionReceiveManager
    {
        private readonly MqttOutgoingMessageManager outgoingMessageManager;

        public void ProcessReceivedMessage(MqttRawMessage rawMessage)
        {
            if (!rawMessage.ClientConnection.isRunning)
            {
                return;
            }

            // update last message received ticks
            rawMessage.ClientConnection.lastCommTime = Environment.TickCount;
            
            // extract message type from received byte
            byte msgType = (byte)((rawMessage.MessageType & MqttMsgBase.MSG_TYPE_MASK) >> MqttMsgBase.MSG_TYPE_OFFSET);
            byte protocolVersion = (byte)rawMessage.ClientConnection.ProtocolVersion;
            switch (msgType)
            {
                case MqttMsgBase.MQTT_MSG_CONNECT_TYPE:
                    MqttMsgConnect connect = MqttMsgConnect.Parse(rawMessage.MessageType, protocolVersion, rawMessage.PayloadBuffer);
                    rawMessage.ClientConnection.EnqueueInternalEvent(new MsgInternalEvent(connect));
                    break;

                case MqttMsgBase.MQTT_MSG_PINGREQ_TYPE:
                    var pingReqest = MqttMsgPingReq.Parse(rawMessage.MessageType, protocolVersion);
                    outgoingMessageManager.PingResp(rawMessage.ClientConnection); 
                    break;

                case MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE:
                    MqttMsgSubscribe subscribe = MqttMsgSubscribe.Parse(rawMessage.MessageType, protocolVersion, rawMessage.PayloadBuffer);
                    rawMessage.ClientConnection.EnqueueInternalEvent(new MsgInternalEvent(subscribe));
                    break;

                case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:
                    MqttMsgPublish publish = MqttMsgPublish.Parse(rawMessage.MessageType, protocolVersion, rawMessage.PayloadBuffer);
                    EnqueueInflight(rawMessage.ClientConnection, publish, MqttMsgFlow.ToAcknowledge);
                    break;

                case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:
                    // enqueue PUBACK message received (for QoS Level 1) into the internal queue
                    MqttMsgPuback puback = MqttMsgPuback.Parse(rawMessage.MessageType, protocolVersion, rawMessage.PayloadBuffer);
                    EnqueueInternal(rawMessage.ClientConnection, puback);
                    break;

                case MqttMsgBase.MQTT_MSG_PUBREC_TYPE:
                    // enqueue PUBREC message received (for QoS Level 2) into the internal queue
                    MqttMsgPubrec pubrec = MqttMsgPubrec.Parse(rawMessage.MessageType, protocolVersion, rawMessage.PayloadBuffer);
                    EnqueueInternal(rawMessage.ClientConnection, pubrec);
                    break;

                case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:
                    // enqueue PUBREL message received (for QoS Level 2) into the internal queue
                    MqttMsgPubrel pubrel = MqttMsgPubrel.Parse(rawMessage.MessageType, protocolVersion, rawMessage.PayloadBuffer);
                    EnqueueInternal(rawMessage.ClientConnection, pubrel);
                    break;

                case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:
                    // enqueue PUBCOMP message received (for QoS Level 2) into the internal queue
                    MqttMsgPubcomp pubcomp = MqttMsgPubcomp.Parse(rawMessage.MessageType, protocolVersion, rawMessage.PayloadBuffer);
                    EnqueueInternal(rawMessage.ClientConnection, pubcomp);
                    break;

                case MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE:
                    MqttMsgUnsubscribe unsubscribe = MqttMsgUnsubscribe.Parse(rawMessage.MessageType, protocolVersion, rawMessage.PayloadBuffer);
                    rawMessage.ClientConnection.EnqueueInternalEvent(new MsgInternalEvent(unsubscribe));
                    break;

                case MqttMsgBase.MQTT_MSG_DISCONNECT_TYPE:
                    MqttMsgDisconnect disconnect = MqttMsgDisconnect.Parse(rawMessage.MessageType, protocolVersion);
                    rawMessage.ClientConnection.EnqueueInternalEvent(new MsgInternalEvent(disconnect));
                    break;

                case MqttMsgBase.MQTT_MSG_CONNACK_TYPE:
                case MqttMsgBase.MQTT_MSG_PINGRESP_TYPE:
                case MqttMsgBase.MQTT_MSG_SUBACK_TYPE:
                case MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE:
                    throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);

                default:
                    throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
            }
        }

        private bool EnqueueInflight(MqttClientConnection clientConnection, MqttMsgBase msg, MqttMsgFlow flow)
        {
            // enqueue is needed (or not)
            bool enqueue = true;

            // if it is a PUBLISH message with QoS Level 2
            if ((msg.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) && (msg.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE))
            {
                // if it is a PUBLISH message already received (it is in the inflight queue), the publisher
                // re-sent it because it didn't received the PUBREC. In clientConnection case, we have to re-send PUBREC

                // NOTE : I need to find on message id and flow because the broker could be publish/received
                //        to/from client and message id could be the same (one tracked by broker and the other by client)
                MqttClientConnection.MqttMsgContextFinder msgCtxFinder =
                    new MqttClientConnection.MqttMsgContextFinder(msg.MessageId, MqttMsgFlow.ToAcknowledge);
                MqttMsgContext msgCtx = (MqttMsgContext)clientConnection.inflightQueue.FirstOrDefault(msgCtxFinder.Find);

                // the PUBLISH message is alredy in the inflight queue, we don't need to re-enqueue but we need
                // to change state to re-send PUBREC
                if (msgCtx != null)
                {
                    msgCtx.State = MqttMsgState.QueuedQos2;
                    msgCtx.Flow = MqttMsgFlow.ToAcknowledge;
                    enqueue = false;
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
                {
                    state = MqttMsgState.SendSubscribe;
                }
                else if (msg.Type == MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE)
                {
                    state = MqttMsgState.SendUnsubscribe;
                }

                // queue message context
                MqttMsgContext msgContext = new MqttMsgContext()
                                                {
                                                    Message = msg,
                                                    State = state,
                                                    Flow = flow,
                                                    Attempt = 0
                                                };

                // enqueue message and unlock send thread
                clientConnection.EnqueueInflight(msgContext);

                // PUBLISH message
                if (msg.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                {
                    // to publish and QoS level 1 or 2
                    if ((msgContext.Flow == MqttMsgFlow.ToPublish)
                        && ((msg.QosLevel == MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE)
                            || (msg.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE)))
                    {
                        if (clientConnection.Session != null)
                        {
                            clientConnection.Session.InflightMessages.Add(msgContext.Key, msgContext);
                        }
                    }
                    // to acknowledge and QoS level 2
                    else if ((msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                             && (msg.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE))
                    {
                        if (clientConnection.Session != null)
                        {
                            clientConnection.Session.InflightMessages.Add(msgContext.Key, msgContext);
                        }
                    }
                }
            }

            return enqueue;
        }

        private void EnqueueInternal(MqttClientConnection clientConnection, MqttMsgBase msg)
        {
            // enqueue is needed (or not)
            bool enqueue = true;

            // if it is a PUBREL message (for QoS Level 2)
            if (msg.Type == MqttMsgBase.MQTT_MSG_PUBREL_TYPE)
            {
                // if it is a PUBREL but the corresponding PUBLISH isn't in the inflight queue,
                // it means that we processed PUBLISH message and received PUBREL and we sent PUBCOMP
                // but publisher didn't receive PUBCOMP so it re-sent PUBREL. We need only to re-send PUBCOMP.

                // NOTE : I need to find on message id and flow because the broker could be publish/received
                //        to/from client and message id could be the same (one tracked by broker and the other by client)
                MqttClientConnection.MqttMsgContextFinder msgCtxFinder =
                    new MqttClientConnection.MqttMsgContextFinder(msg.MessageId, MqttMsgFlow.ToAcknowledge);
                MqttMsgContext msgCtx = (MqttMsgContext)clientConnection.inflightQueue.FirstOrDefault(msgCtxFinder.Find);

                // the PUBLISH message isn't in the inflight queue, it was already processed so
                // we need to re-send PUBCOMP only
                if (msgCtx == null)
                {
                    outgoingMessageManager.Pubcomp(clientConnection, msg.MessageId);
                    enqueue = false;
                }
            }
            // if it is a PUBCOMP message (for QoS Level 2)
            else if (msg.Type == MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE)
            {
                // if it is a PUBCOMP but the corresponding PUBLISH isn't in the inflight queue,
                // it means that we sent PUBLISH message, sent PUBREL (after receiving PUBREC) and already received PUBCOMP
                // but publisher didn't receive PUBREL so it re-sent PUBCOMP. We need only to ignore clientConnection PUBCOMP.

                // NOTE : I need to find on message id and flow because the broker could be publish/received
                //        to/from client and message id could be the same (one tracked by broker and the other by client)
                MqttClientConnection.MqttMsgContextFinder msgCtxFinder =
                    new MqttClientConnection.MqttMsgContextFinder(msg.MessageId, MqttMsgFlow.ToPublish);
                MqttMsgContext msgCtx = (MqttMsgContext)clientConnection.inflightQueue.FirstOrDefault(msgCtxFinder.Find);

                // the PUBLISH message isn't in the inflight queue, it was already sent so we need to ignore clientConnection PUBCOMP
                if (msgCtx == null)
                {
                    enqueue = false;
                }
            }
            // if it is a PUBREC message (for QoS Level 2)
            else if (msg.Type == MqttMsgBase.MQTT_MSG_PUBREC_TYPE)
            {
                // if it is a PUBREC but the corresponding PUBLISH isn't in the inflight queue,
                // it means that we sent PUBLISH message more times (retries) but broker didn't send PUBREC in time
                // the publish is failed and we need only to ignore rawMessage.ClientConnection PUBREC.

                // NOTE : I need to find on message id and flow because the broker could be publish/received
                //        to/from client and message id could be the same (one tracked by broker and the other by client)
                MqttClientConnection.MqttMsgContextFinder msgCtxFinder =
                    new MqttClientConnection.MqttMsgContextFinder(msg.MessageId, MqttMsgFlow.ToPublish);
                MqttMsgContext msgCtx = (MqttMsgContext)clientConnection.inflightQueue.FirstOrDefault(msgCtxFinder.Find);

                // the PUBLISH message isn't in the inflight queue, it was already sent so we need to ignore rawMessage.ClientConnection PUBREC
                if (msgCtx == null)
                {
                    enqueue = false;
                }
            }

            if (enqueue)
            {
                clientConnection.internalQueue.Enqueue(msg);
            }
        }
    }
}