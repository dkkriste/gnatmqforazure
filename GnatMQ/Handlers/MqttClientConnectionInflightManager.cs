namespace GnatMQForAzure.Handlers
{
    using System;
    using System.Threading;

    using GnatMQForAzure.Entities.Enums;
    using GnatMQForAzure.Events;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Managers;
    using GnatMQForAzure.Messages;

    public class MqttClientConnectionInflightManager
    {
        private readonly MqttOutgoingMessageManager outgoingMessageManager;

        public void ProcessInflightQueue(MqttClientConnection clientConnection)
        {
            if (!clientConnection.isRunning)
            {
                return;
            }

            MqttMsgBase msgReceived = null;
            bool msgReceivedProcessed = false;

            //TODO fix timeout

            // message received and peeked from internal queue is processed
            // NOTE : it has the corresponding message in inflight queue based on messageId
            //        (ex. a PUBREC for a PUBLISH, a SUBACK for a SUBSCRIBE, ...)
            //        if it's orphan we need to remove from internal queue

            // set timeout tu MaxValue instead of Infinte (-1) to perform
            // compare with calcultad current msgTimeout
            int timeout = Int32.MaxValue;

            // a message inflight could be re-enqueued but we have to
            // analyze it only just one time for cycle
            int count = clientConnection.inflightQueue.Count;
            // process all inflight queued messages
            while (count > 0 && clientConnection.isRunning)
            {
                count--;
                msgReceived = null;

                // dequeue message context from queue
                MqttMsgContext msgContext;
                if (!clientConnection.inflightQueue.TryDequeue(out msgContext))
                {
                    return;
                }

                // get inflight message
                var msgInflight = (MqttMsgBase)msgContext.Message;

                switch (msgContext.State)
                {
                    case MqttMsgState.QueuedQos0:
                        HandleQueuedQos0(clientConnection, msgContext, msgInflight);
                        break;

                    // [v3.1.1] SUBSCRIBE and UNSIBSCRIBE aren't "officially" QOS = 1
                    case MqttMsgState.QueuedQos1:
                    case MqttMsgState.SendSubscribe:
                    case MqttMsgState.SendUnsubscribe:
                        timeout = HandleQueuedQos1SendSubscribeAndSendUnsubscribe(clientConnection, msgContext, msgInflight, timeout);
                        break;

                    case MqttMsgState.QueuedQos2:
                        timeout = HandleQueuedQos2(clientConnection, msgContext, msgInflight, timeout);
                        break;

                    case MqttMsgState.WaitForPuback:
                    case MqttMsgState.WaitForSuback:
                    case MqttMsgState.WaitForUnsuback:
                        msgReceived = HandleWaitForPubackSubackUbsuback(clientConnection, msgContext, msgInflight, ref msgReceivedProcessed, ref timeout);
                        break;

                    case MqttMsgState.WaitForPubrec:
                        msgReceived = HandleWaitForPubrec(clientConnection, msgContext, msgInflight, ref msgReceivedProcessed, ref timeout);
                        break;

                    case MqttMsgState.WaitForPubrel:
                        msgReceived = WaitForPubrel(clientConnection, msgContext, msgInflight, ref msgReceivedProcessed);
                        break;

                    case MqttMsgState.WaitForPubcomp:
                        msgReceived = WaitForPubcomp(clientConnection, msgContext, msgInflight, ref msgReceivedProcessed, ref timeout);
                        break;

                    case MqttMsgState.SendPubrec:
                        // TODO : impossible ? --> QueuedQos2 ToAcknowledge
                        break;

                    case MqttMsgState.SendPubrel:
                        timeout = SendPubrel(clientConnection, msgContext, msgInflight, timeout);
                        break;

                    case MqttMsgState.SendPubcomp:
                        // TODO : impossible ?
                        break;
                    case MqttMsgState.SendPuback:
                        // TODO : impossible ? --> QueuedQos1 ToAcknowledge
                        break;
                    default:
                        break;
                }
            }

            // if calculated timeout is MaxValue, it means that must be Infinite (-1)
            if (timeout == Int32.MaxValue) timeout = Timeout.Infinite;

            // if message received is orphan, no corresponding message in inflight queue
            // based on messageId, we need to remove from the queue
            if ((msgReceived != null) && !msgReceivedProcessed)
            {
                MqttMsgBase dequeuedMsg;
                clientConnection.internalQueue.TryDequeue(out dequeuedMsg);
            }
        }

        private int SendPubrel(MqttClientConnection clientConnection, MqttMsgContext msgContext, MqttMsgBase msgInflight, int timeout)
        {
            // QoS 2, PUBREL message to send to broker, state change to wait PUBCOMP
            if (msgContext.Flow == MqttMsgFlow.ToPublish)
            {
                msgContext.State = MqttMsgState.WaitForPubcomp;
                msgContext.Timestamp = Environment.TickCount;
                msgContext.Attempt++;
                // retry ? set dup flag [v3.1.1] no needed
                if (clientConnection.ProtocolVersion == MqttProtocolVersion.Version_3_1 && msgContext.Attempt > 1)
                {
                    outgoingMessageManager.Pubrel(clientConnection, msgInflight.MessageId, true);
                }
                else
                {
                    outgoingMessageManager.Pubrel(clientConnection, msgInflight.MessageId, false);
                }

                // update timeout : minimum between delay (based on current message sent) or current timeout
                timeout = (clientConnection.Settings.DelayOnRetry < timeout) ? clientConnection.Settings.DelayOnRetry : timeout;

                // re-enqueue message
                clientConnection.EnqueueInflight(msgContext);
            }

            return timeout;
        }

        private MqttMsgBase WaitForPubcomp(
            MqttClientConnection clientConnection,
            MqttMsgContext msgContext,
            MqttMsgBase msgInflight,
            ref bool msgReceivedProcessed,
            ref int timeout)
        {
            // QoS 2, waiting for PUBCOMP of a PUBREL message sent
            if (msgContext.Flow != MqttMsgFlow.ToPublish)
            {
                return null;
            }

            MqttMsgBase msgReceived;
            if (!clientConnection.internalQueue.TryPeek(out msgReceived))
            {
                return null;
            }

            bool acknowledge = false;
            InternalEvent internalEvent;

            // it is a PUBCOMP message
            if (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE)
            {
                // PUBCOMP message for the current message
                if (msgReceived.MessageId == msgInflight.MessageId)
                {
                    MqttMsgBase dequeuedMsg;
                    clientConnection.internalQueue.TryDequeue(out dequeuedMsg);
                    acknowledge = true;
                    msgReceivedProcessed = true;

                    internalEvent = new MsgPublishedInternalEvent(msgReceived, true);
                    // notify received acknowledge from broker of a published message
                    clientConnection.EnqueueInternalEvent(internalEvent);

                    // PUBCOMP received for PUBLISH message with QoS Level 2, remove from session state
                    if (msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE 
                        && clientConnection.Session != null
                        && clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key))
                    {
                        clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                    }
                }
            }
            // it is a PUBREC message
            else if (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBREC_TYPE)
            {
                // another PUBREC message for the current message due to a retransmitted PUBLISH
                // I'm in waiting for PUBCOMP, so I can discard clientConnection PUBREC
                if (msgReceived.MessageId == msgInflight.MessageId)
                {
                    MqttMsgBase dequeuedMsg;
                    clientConnection.internalQueue.TryDequeue(out dequeuedMsg);
                    acknowledge = true;
                    msgReceivedProcessed = true;

                    // re-enqueue message
                    clientConnection.EnqueueInflight(msgContext);
                }
            }

            // current message not acknowledged
            if (!acknowledge)
            {
                var delta = Environment.TickCount - msgContext.Timestamp;
                // check timeout for receiving PUBCOMP since PUBREL was sent
                if (delta >= clientConnection.Settings.DelayOnRetry)
                {
                    // max retry not reached, resend
                    if (msgContext.Attempt < clientConnection.Settings.AttemptsOnRetry)
                    {
                        msgContext.State = MqttMsgState.SendPubrel;

                        // re-enqueue message
                        clientConnection.EnqueueInflight(msgContext);

                        // update timeout (0 -> reanalyze queue immediately)
                        timeout = 0;
                    }
                    else
                    {
                        // PUBCOMP not received, PUBREL retries failed, need to remove from session inflight messages too
                        if ((clientConnection.Session != null) && clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key))
                        {
                            clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                        }

                        // if PUBCOMP for a PUBLISH message not received after retries, raise event for not published
                        internalEvent = new MsgPublishedInternalEvent(msgInflight, false);
                        // notify not received acknowledge from broker and message not published
                        clientConnection.EnqueueInternalEvent(internalEvent);
                    }
                }
                else
                {
                    // re-enqueue message
                    clientConnection.EnqueueInflight(msgContext);

                    // update timeout
                    int msgTimeout = clientConnection.Settings.DelayOnRetry - delta;
                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                }
            }

            return msgReceived;
        }

        private MqttMsgBase WaitForPubrel(MqttClientConnection clientConnection, MqttMsgContext msgContext, MqttMsgBase msgInflight, ref bool msgReceivedProcessed)
        {
            // QoS 2, waiting for PUBREL of a PUBREC message sent
            if (msgContext.Flow != MqttMsgFlow.ToAcknowledge)
            {
                return null;
            }

            MqttMsgBase msgReceived;
            if (!clientConnection.internalQueue.TryPeek(out msgReceived))
            {
                return null;
            }

            InternalEvent internalEvent;

            // it is a PUBREL message
            if ((msgReceived != null) && (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBREL_TYPE))
            {
                // PUBREL message for the current message, send PUBCOMP
                if (msgReceived.MessageId == msgInflight.MessageId)
                {
                    // received message processed
                    MqttMsgBase dequeuedMsg;
                    clientConnection.internalQueue.TryDequeue(out dequeuedMsg);
                    msgReceivedProcessed = true;


                    outgoingMessageManager.Pubcomp(clientConnection, msgInflight.MessageId);
                   

                    internalEvent = new MsgInternalEvent(msgInflight);
                    // notify published message from broker and acknowledged
                    clientConnection.EnqueueInternalEvent(internalEvent);

                    // PUBREL received (and PUBCOMP sent) for PUBLISH message with QoS Level 2, remove from session state
                    if ((msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) && (clientConnection.Session != null)
                        && (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                    {
                        clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                    }
                }
                else
                {
                    // re-enqueue message
                    clientConnection.EnqueueInflight(msgContext);
                }
            }
            else
            {
                // re-enqueue message
                clientConnection.EnqueueInflight(msgContext);
            }

            return msgReceived;
        }

        private MqttMsgBase HandleWaitForPubrec(
            MqttClientConnection clientConnection,
            MqttMsgContext msgContext,
            MqttMsgBase msgInflight,
            ref bool msgReceivedProcessed,
            ref int timeout)
        {
            // QoS 2, waiting for PUBREC of a PUBLISH message sent
            if (msgContext.Flow != MqttMsgFlow.ToPublish)
            {
                return null;
            }

            MqttMsgBase msgReceived;
            if (!clientConnection.internalQueue.TryPeek(out msgReceived))
            {
                return null;
            }

            bool acknowledge = false;
            InternalEvent internalEvent;

            // it is a PUBREC message
            if (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBREC_TYPE)
            {
                // PUBREC message for the current PUBLISH message, send PUBREL, wait for PUBCOMP
                if (msgReceived.MessageId == msgInflight.MessageId)
                {
                    // received message processed
                    MqttMsgBase dequeuedMsg;
                    clientConnection.internalQueue.TryDequeue(out dequeuedMsg);
                    acknowledge = true;
                    msgReceivedProcessed = true;

                    outgoingMessageManager.Pubrel(clientConnection, msgInflight.MessageId, false);

                    msgContext.State = MqttMsgState.WaitForPubcomp;
                    msgContext.Timestamp = Environment.TickCount;
                    msgContext.Attempt = 1;

                    // update timeout : minimum between delay (based on current message sent) or current timeout
                    timeout = (clientConnection.Settings.DelayOnRetry < timeout)
                                  ? clientConnection.Settings.DelayOnRetry
                                  : timeout;

                    // re-enqueue message
                    clientConnection.EnqueueInflight(msgContext);
                }
            }

            // current message not acknowledged
            if (!acknowledge)
            {
                var delta = Environment.TickCount - msgContext.Timestamp;
                // check timeout for receiving PUBREC since PUBLISH was sent
                if (delta >= clientConnection.Settings.DelayOnRetry)
                {
                    // max retry not reached, resend
                    if (msgContext.Attempt < clientConnection.Settings.AttemptsOnRetry)
                    {
                        msgContext.State = MqttMsgState.QueuedQos2;

                        // re-enqueue message
                        clientConnection.EnqueueInflight(msgContext);

                        // update timeout (0 -> reanalyze queue immediately)
                        timeout = 0;
                    }
                    else
                    {
                        // PUBREC not received in time, PUBLISH retries failed, need to remove from session inflight messages too
                        if ((clientConnection.Session != null)
                            && (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                        {
                            clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                        }

                        // if PUBREC for a PUBLISH message not received after retries, raise event for not published
                        internalEvent = new MsgPublishedInternalEvent(msgInflight, false);
                        // notify not received acknowledge from broker and message not published
                        clientConnection.EnqueueInternalEvent(internalEvent);
                    }
                }
                else
                {
                    // re-enqueue message
                    clientConnection.EnqueueInflight(msgContext);

                    // update timeout
                    int msgTimeout = (clientConnection.Settings.DelayOnRetry - delta);
                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                }
            }

            return msgReceived;
        }

        private MqttMsgBase HandleWaitForPubackSubackUbsuback(
            MqttClientConnection clientConnection,
            MqttMsgContext msgContext,
            MqttMsgBase msgInflight,
            ref bool msgReceivedProcessed,
            ref int timeout)
        {
            // QoS 1, waiting for PUBACK of a PUBLISH message sent or
            //        waiting for SUBACK of a SUBSCRIBE message sent or
            //        waiting for UNSUBACK of a UNSUBSCRIBE message sent or
            if (msgContext.Flow != MqttMsgFlow.ToPublish)
            {
                return null;
            }

            MqttMsgBase msgReceived;
            if (!clientConnection.internalQueue.TryPeek(out msgReceived))
            {
                return null;
            }

            bool acknowledge = false;
            InternalEvent internalEvent;

            // PUBACK message or SUBACK/UNSUBACK message for the current message
            if (((msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBACK_TYPE)
                 && (msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                 && (msgReceived.MessageId == msgInflight.MessageId))
                || ((msgReceived.Type == MqttMsgBase.MQTT_MSG_SUBACK_TYPE)
                    && (msgInflight.Type == MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE)
                    && (msgReceived.MessageId == msgInflight.MessageId))
                || ((msgReceived.Type == MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE)
                    && (msgInflight.Type == MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE)
                    && (msgReceived.MessageId == msgInflight.MessageId)))
            {
                // received message processed
                MqttMsgBase dequeuedMsg;
                clientConnection.internalQueue.TryDequeue(out dequeuedMsg);
                acknowledge = true;
                msgReceivedProcessed = true;

                // if PUBACK received, confirm published with flag
                if (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBACK_TYPE)
                {
                    internalEvent = new MsgPublishedInternalEvent(msgReceived, true);
                }
                else
                {
                    internalEvent = new MsgInternalEvent(msgReceived);
                }

                // notify received acknowledge from broker of a published message or subscribe/unsubscribe message
                clientConnection.EnqueueInternalEvent(internalEvent);

                // PUBACK received for PUBLISH message with QoS Level 1, remove from session state
                if ((msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) && (clientConnection.Session != null)
                    && (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                {
                    clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                }
            }

            // current message not acknowledged, no PUBACK or SUBACK/UNSUBACK or not equal messageid 
            if (!acknowledge)
            {
                var delta = Environment.TickCount - msgContext.Timestamp;
                // check timeout for receiving PUBACK since PUBLISH was sent or
                // for receiving SUBACK since SUBSCRIBE was sent or
                // for receiving UNSUBACK since UNSUBSCRIBE was sent
                if (delta >= clientConnection.Settings.DelayOnRetry)
                {
                    // max retry not reached, resend
                    if (msgContext.Attempt < clientConnection.Settings.AttemptsOnRetry)
                    {
                        msgContext.State = MqttMsgState.QueuedQos1;

                        // re-enqueue message
                        clientConnection.EnqueueInflight(msgContext);

                        // update timeout (0 -> reanalyze queue immediately)
                        timeout = 0;
                    }
                    else
                    {
                        // if PUBACK for a PUBLISH message not received after retries, raise event for not published
                        if (msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                        {
                            // PUBACK not received in time, PUBLISH retries failed, need to remove from session inflight messages too
                            if ((clientConnection.Session != null)
                                && (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                            {
                                clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                            }

                            internalEvent = new MsgPublishedInternalEvent(msgInflight, false);

                            // notify not received acknowledge from broker and message not published
                            clientConnection.EnqueueInternalEvent(internalEvent);
                        }
                        // NOTE : not raise events for SUBACK or UNSUBACK not received
                        //        for the user no event raised means subscribe/unsubscribe failed
                    }
                }
                else
                {
                    // re-enqueue message (I have to re-analyze for receiving PUBACK, SUBACK or UNSUBACK)
                    clientConnection.EnqueueInflight(msgContext);

                    // update timeout
                    int msgTimeout = (clientConnection.Settings.DelayOnRetry - delta);
                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                }
            }

            return msgReceived;
        }

        private int HandleQueuedQos2(
            MqttClientConnection clientConnection,
            MqttMsgContext msgContext,
            MqttMsgBase msgInflight,
            int timeout)
        {
            // QoS 2, PUBLISH message to send to broker, state change to wait PUBREC
            if (msgContext.Flow == MqttMsgFlow.ToPublish)
            {
                msgContext.Timestamp = Environment.TickCount;
                msgContext.Attempt++;
                msgContext.State = MqttMsgState.WaitForPubrec;
                // retry ? set dup flag
                if (msgContext.Attempt > 1)
                {
                    msgInflight.DupFlag = true;
                }

                outgoingMessageManager.Send(clientConnection, msgInflight);

                // update timeout : minimum between delay (based on current message sent) or current timeout
                timeout = (clientConnection.Settings.DelayOnRetry < timeout) ? clientConnection.Settings.DelayOnRetry : timeout;

                // re-enqueue message (I have to re-analyze for receiving PUBREC)
                clientConnection.EnqueueInflight(msgContext);
            }
            // QoS 2, PUBLISH message received from broker to acknowledge, send PUBREC, state change to wait PUBREL
            else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
            {
                msgContext.State = MqttMsgState.WaitForPubrel;

                outgoingMessageManager.Pubrec(clientConnection, msgInflight.MessageId);

                // re-enqueue message (I have to re-analyze for receiving PUBREL)
                clientConnection.EnqueueInflight(msgContext);
            }

            return timeout;
        }

        private int HandleQueuedQos1SendSubscribeAndSendUnsubscribe(
            MqttClientConnection clientConnection,
            MqttMsgContext msgContext,
            MqttMsgBase msgInflight,
            int timeout)
        {
            InternalEvent internalEvent;
            // QoS 1, PUBLISH or SUBSCRIBE/UNSUBSCRIBE message to send to broker, state change to wait PUBACK or SUBACK/UNSUBACK
            if (msgContext.Flow == MqttMsgFlow.ToPublish)
            {
                msgContext.Timestamp = Environment.TickCount;
                msgContext.Attempt++;

                if (msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                {
                    // PUBLISH message to send, wait for PUBACK
                    msgContext.State = MqttMsgState.WaitForPuback;
                    // retry ? set dup flag [v3.1.1] only for PUBLISH message
                    if (msgContext.Attempt > 1)
                    {
                        msgInflight.DupFlag = true;
                    }
                }
                else if (msgInflight.Type == MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE)
                {
                    // SUBSCRIBE message to send, wait for SUBACK
                    msgContext.State = MqttMsgState.WaitForSuback;
                }
                else if (msgInflight.Type == MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE)
                {
                    // UNSUBSCRIBE message to send, wait for UNSUBACK
                    msgContext.State = MqttMsgState.WaitForUnsuback;
                }

                outgoingMessageManager.Send(clientConnection, msgInflight);

                // update timeout : minimum between delay (based on current message sent) or current timeout
                timeout = (clientConnection.Settings.DelayOnRetry < timeout) ? clientConnection.Settings.DelayOnRetry : timeout;

                // re-enqueue message (I have to re-analyze for receiving PUBACK, SUBACK or UNSUBACK)
                clientConnection.EnqueueInflight(msgContext);
            }
            // QoS 1, PUBLISH message received from broker to acknowledge, send PUBACK
            else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
            {
                outgoingMessageManager.Puback(clientConnection, msgInflight.MessageId);

                internalEvent = new MsgInternalEvent(msgInflight);
                // notify published message from broker and acknowledged
                clientConnection.EnqueueInternalEvent(internalEvent);
            }
            return timeout;
        }

        private void HandleQueuedQos0(
            MqttClientConnection clientConnection,
            MqttMsgContext msgContext,
            MqttMsgBase msgInflight)
        {
            // QoS 0, PUBLISH message to send to broker, no state change, no acknowledge
            if (msgContext.Flow == MqttMsgFlow.ToPublish)
            {
                outgoingMessageManager.Send(clientConnection, msgInflight);
            }
            // QoS 0, no need acknowledge
            else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
            {
                var internalEvent = new MsgInternalEvent(msgInflight);
                clientConnection.EnqueueInternalEvent(internalEvent);
            }
        }
    }
}