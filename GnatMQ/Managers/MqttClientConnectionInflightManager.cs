namespace GnatMQForAzure.Managers
{
    using System;
    using System.Threading;

    using GnatMQForAzure.Enums;
    using GnatMQForAzure.Events;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Messages;

    public class MqttClientConnectionInflightManager
    {
        /// <summary>
        /// Process inflight messages queue
        /// </summary>
        private void ProcessInflightThread(MqttClientConnection clientConnection)
        {
            MqttMsgContext msgContext = null;
            MqttMsgBase msgInflight = null;
            MqttMsgBase msgReceived = null;
            InternalEvent internalEvent = null;
            bool acknowledge = false;
            int timeout = Timeout.Infinite;
            int delta;
            bool msgReceivedProcessed = false;

            try
            {
                while (clientConnection.isRunning)
                {
                    // wait on message queueud to inflight
                    clientConnection.inflightWaitHandle.WaitOne(timeout);

                    // it could be unblocked because Close() method is joining
                    if (clientConnection.isRunning)
                    {
                        lock (clientConnection.inflightQueue)
                        {
                            // message received and peeked from internal queue is processed
                            // NOTE : it has the corresponding message in inflight queue based on messageId
                            //        (ex. a PUBREC for a PUBLISH, a SUBACK for a SUBSCRIBE, ...)
                            //        if it's orphan we need to remove from internal queue
                            msgReceivedProcessed = false;
                            acknowledge = false;
                            msgReceived = null;

                            // set timeout tu MaxValue instead of Infinte (-1) to perform
                            // compare with calcultad current msgTimeout
                            timeout = Int32.MaxValue;

                            // a message inflight could be re-enqueued but we have to
                            // analyze it only just one time for cycle
                            int count = clientConnection.inflightQueue.Count;
                            // process all inflight queued messages
                            while (count > 0)
                            {
                                count--;
                                acknowledge = false;
                                msgReceived = null;

                                // check to be sure that client isn't closing and all queues are now empty !
                                if (!clientConnection.isRunning)
                                    break;

                                // dequeue message context from queue
                                msgContext = (MqttMsgContext)clientConnection.inflightQueue.Dequeue();

                                // get inflight message
                                msgInflight = (MqttMsgBase)msgContext.Message;

                                switch (msgContext.State)
                                {
                                    case MqttMsgState.QueuedQos0:

                                        // QoS 0, PUBLISH message to send to broker, no state change, no acknowledge
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            clientConnection.Send(msgInflight);
                                        }
                                        // QoS 0, no need acknowledge
                                        else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                                        {
                                            internalEvent = new MsgInternalEvent(msgInflight);
                                            // notify published message from broker (no need acknowledged)
                                            clientConnection.OnInternalEvent(internalEvent);
                                        }

                                        break;

                                    case MqttMsgState.QueuedQos1:
                                    // [v3.1.1] SUBSCRIBE and UNSIBSCRIBE aren't "officially" QOS = 1
                                    case MqttMsgState.SendSubscribe:
                                    case MqttMsgState.SendUnsubscribe:

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
                                                    msgInflight.DupFlag = true;
                                            }
                                            else if (msgInflight.Type == MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE)
                                                // SUBSCRIBE message to send, wait for SUBACK
                                                msgContext.State = MqttMsgState.WaitForSuback;
                                            else if (msgInflight.Type == MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE)
                                                // UNSUBSCRIBE message to send, wait for UNSUBACK
                                                msgContext.State = MqttMsgState.WaitForUnsuback;

                                            clientConnection.Send(msgInflight);

                                            // update timeout : minimum between delay (based on current message sent) or current timeout
                                            timeout = (clientConnection.Settings.DelayOnRetry < timeout) ? clientConnection.Settings.DelayOnRetry : timeout;

                                            // re-enqueue message (I have to re-analyze for receiving PUBACK, SUBACK or UNSUBACK)
                                            clientConnection.inflightQueue.Enqueue(msgContext);
                                        }
                                        // QoS 1, PUBLISH message received from broker to acknowledge, send PUBACK
                                        else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                                        {
                                            MqttMsgPuback puback = new MqttMsgPuback();
                                            puback.MessageId = msgInflight.MessageId;

                                            clientConnection.Send(puback);

                                            internalEvent = new MsgInternalEvent(msgInflight);
                                            // notify published message from broker and acknowledged
                                            clientConnection.OnInternalEvent(internalEvent);

                                        }
                                        break;

                                    case MqttMsgState.QueuedQos2:

                                        // QoS 2, PUBLISH message to send to broker, state change to wait PUBREC
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            msgContext.Timestamp = Environment.TickCount;
                                            msgContext.Attempt++;
                                            msgContext.State = MqttMsgState.WaitForPubrec;
                                            // retry ? set dup flag
                                            if (msgContext.Attempt > 1)
                                                msgInflight.DupFlag = true;

                                            clientConnection.Send(msgInflight);

                                            // update timeout : minimum between delay (based on current message sent) or current timeout
                                            timeout = (clientConnection.Settings.DelayOnRetry < timeout) ? clientConnection.Settings.DelayOnRetry : timeout;

                                            // re-enqueue message (I have to re-analyze for receiving PUBREC)
                                            clientConnection.inflightQueue.Enqueue(msgContext);
                                        }
                                        // QoS 2, PUBLISH message received from broker to acknowledge, send PUBREC, state change to wait PUBREL
                                        else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                                        {
                                            MqttMsgPubrec pubrec = new MqttMsgPubrec();
                                            pubrec.MessageId = msgInflight.MessageId;

                                            msgContext.State = MqttMsgState.WaitForPubrel;

                                            clientConnection.Send(pubrec);

                                            // re-enqueue message (I have to re-analyze for receiving PUBREL)
                                            clientConnection.inflightQueue.Enqueue(msgContext);
                                        }
                                        break;

                                    case MqttMsgState.WaitForPuback:
                                    case MqttMsgState.WaitForSuback:
                                    case MqttMsgState.WaitForUnsuback:

                                        // QoS 1, waiting for PUBACK of a PUBLISH message sent or
                                        //        waiting for SUBACK of a SUBSCRIBE message sent or
                                        //        waiting for UNSUBACK of a UNSUBSCRIBE message sent or
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            acknowledge = false;
                                            lock (clientConnection.internalQueue)
                                            {
                                                if (clientConnection.internalQueue.Count > 0)
                                                    msgReceived = (MqttMsgBase)clientConnection.internalQueue.Peek();
                                            }

                                            // it is a PUBACK message or a SUBACK/UNSUBACK message
                                            if (msgReceived != null)
                                            {
                                                // PUBACK message or SUBACK/UNSUBACK message for the current message
                                                if (((msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBACK_TYPE) && (msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) && (msgReceived.MessageId == msgInflight.MessageId)) ||
                                                    ((msgReceived.Type == MqttMsgBase.MQTT_MSG_SUBACK_TYPE) && (msgInflight.Type == MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE) && (msgReceived.MessageId == msgInflight.MessageId)) ||
                                                    ((msgReceived.Type == MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE) && (msgInflight.Type == MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE) && (msgReceived.MessageId == msgInflight.MessageId)))
                                                {
                                                    lock (clientConnection.internalQueue)
                                                    {
                                                        // received message processed
                                                        clientConnection.internalQueue.Dequeue();
                                                        acknowledge = true;
                                                        msgReceivedProcessed = true;
                                                    }

                                                    // if PUBACK received, confirm published with flag
                                                    if (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBACK_TYPE)
                                                        internalEvent = new MsgPublishedInternalEvent(msgReceived, true);
                                                    else
                                                        internalEvent = new MsgInternalEvent(msgReceived);

                                                    // notify received acknowledge from broker of a published message or subscribe/unsubscribe message
                                                    clientConnection.OnInternalEvent(internalEvent);

                                                    // PUBACK received for PUBLISH message with QoS Level 1, remove from session state
                                                    if ((msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) &&
                                                        (clientConnection.Session != null) &&
                                                        (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                                                    {
                                                        clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                                                    }

                                                }
                                            }

                                            // current message not acknowledged, no PUBACK or SUBACK/UNSUBACK or not equal messageid 
                                            if (!acknowledge)
                                            {
                                                delta = Environment.TickCount - msgContext.Timestamp;
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
                                                        clientConnection.inflightQueue.Enqueue(msgContext);

                                                        // update timeout (0 -> reanalyze queue immediately)
                                                        timeout = 0;
                                                    }
                                                    else
                                                    {
                                                        // if PUBACK for a PUBLISH message not received after retries, raise event for not published
                                                        if (msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                                                        {
                                                            // PUBACK not received in time, PUBLISH retries failed, need to remove from session inflight messages too
                                                            if ((clientConnection.Session != null) &&
                                                                (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                                                            {
                                                                clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                                                            }

                                                            internalEvent = new MsgPublishedInternalEvent(msgInflight, false);

                                                            // notify not received acknowledge from broker and message not published
                                                            clientConnection.OnInternalEvent(internalEvent);
                                                        }
                                                        // NOTE : not raise events for SUBACK or UNSUBACK not received
                                                        //        for the user no event raised means subscribe/unsubscribe failed
                                                    }
                                                }
                                                else
                                                {
                                                    // re-enqueue message (I have to re-analyze for receiving PUBACK, SUBACK or UNSUBACK)
                                                    clientConnection.inflightQueue.Enqueue(msgContext);

                                                    // update timeout
                                                    int msgTimeout = (clientConnection.Settings.DelayOnRetry - delta);
                                                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                                                }
                                            }
                                        }
                                        break;

                                    case MqttMsgState.WaitForPubrec:

                                        // QoS 2, waiting for PUBREC of a PUBLISH message sent
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            acknowledge = false;
                                            lock (clientConnection.internalQueue)
                                            {
                                                if (clientConnection.internalQueue.Count > 0)
                                                    msgReceived = (MqttMsgBase)clientConnection.internalQueue.Peek();
                                            }

                                            // it is a PUBREC message
                                            if ((msgReceived != null) && (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBREC_TYPE))
                                            {
                                                // PUBREC message for the current PUBLISH message, send PUBREL, wait for PUBCOMP
                                                if (msgReceived.MessageId == msgInflight.MessageId)
                                                {
                                                    lock (clientConnection.internalQueue)
                                                    {
                                                        // received message processed
                                                        clientConnection.internalQueue.Dequeue();
                                                        acknowledge = true;
                                                        msgReceivedProcessed = true;
                                                    }

                                                    MqttMsgPubrel pubrel = new MqttMsgPubrel();
                                                    pubrel.MessageId = msgInflight.MessageId;

                                                    msgContext.State = MqttMsgState.WaitForPubcomp;
                                                    msgContext.Timestamp = Environment.TickCount;
                                                    msgContext.Attempt = 1;

                                                    clientConnection.Send(pubrel);

                                                    // update timeout : minimum between delay (based on current message sent) or current timeout
                                                    timeout = (clientConnection.Settings.DelayOnRetry < timeout) ? clientConnection.Settings.DelayOnRetry : timeout;

                                                    // re-enqueue message
                                                    clientConnection.inflightQueue.Enqueue(msgContext);
                                                }
                                            }

                                            // current message not acknowledged
                                            if (!acknowledge)
                                            {
                                                delta = Environment.TickCount - msgContext.Timestamp;
                                                // check timeout for receiving PUBREC since PUBLISH was sent
                                                if (delta >= clientConnection.Settings.DelayOnRetry)
                                                {
                                                    // max retry not reached, resend
                                                    if (msgContext.Attempt < clientConnection.Settings.AttemptsOnRetry)
                                                    {
                                                        msgContext.State = MqttMsgState.QueuedQos2;

                                                        // re-enqueue message
                                                        clientConnection.inflightQueue.Enqueue(msgContext);

                                                        // update timeout (0 -> reanalyze queue immediately)
                                                        timeout = 0;
                                                    }
                                                    else
                                                    {
                                                        // PUBREC not received in time, PUBLISH retries failed, need to remove from session inflight messages too
                                                        if ((clientConnection.Session != null) &&
                                                            (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                                                        {
                                                            clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                                                        }

                                                        // if PUBREC for a PUBLISH message not received after retries, raise event for not published
                                                        internalEvent = new MsgPublishedInternalEvent(msgInflight, false);
                                                        // notify not received acknowledge from broker and message not published
                                                        clientConnection.OnInternalEvent(internalEvent);
                                                    }
                                                }
                                                else
                                                {
                                                    // re-enqueue message
                                                    clientConnection.inflightQueue.Enqueue(msgContext);

                                                    // update timeout
                                                    int msgTimeout = (clientConnection.Settings.DelayOnRetry - delta);
                                                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                                                }
                                            }
                                        }
                                        break;

                                    case MqttMsgState.WaitForPubrel:

                                        // QoS 2, waiting for PUBREL of a PUBREC message sent
                                        if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                                        {
                                            lock (clientConnection.internalQueue)
                                            {
                                                if (clientConnection.internalQueue.Count > 0)
                                                    msgReceived = (MqttMsgBase)clientConnection.internalQueue.Peek();
                                            }

                                            // it is a PUBREL message
                                            if ((msgReceived != null) && (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBREL_TYPE))
                                            {
                                                // PUBREL message for the current message, send PUBCOMP
                                                if (msgReceived.MessageId == msgInflight.MessageId)
                                                {
                                                    lock (clientConnection.internalQueue)
                                                    {
                                                        // received message processed
                                                        clientConnection.internalQueue.Dequeue();
                                                        msgReceivedProcessed = true;
                                                    }

                                                    MqttMsgPubcomp pubcomp = new MqttMsgPubcomp();
                                                    pubcomp.MessageId = msgInflight.MessageId;

                                                    clientConnection.Send(pubcomp);

                                                    internalEvent = new MsgInternalEvent(msgInflight);
                                                    // notify published message from broker and acknowledged
                                                    clientConnection.OnInternalEvent(internalEvent);

                                                    // PUBREL received (and PUBCOMP sent) for PUBLISH message with QoS Level 2, remove from session state
                                                    if ((msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) &&
                                                        (clientConnection.Session != null) &&
                                                        (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                                                    {
                                                        clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                                                    }
                                                }
                                                else
                                                {
                                                    // re-enqueue message
                                                    clientConnection.inflightQueue.Enqueue(msgContext);
                                                }
                                            }
                                            else
                                            {
                                                // re-enqueue message
                                                clientConnection.inflightQueue.Enqueue(msgContext);
                                            }
                                        }
                                        break;

                                    case MqttMsgState.WaitForPubcomp:

                                        // QoS 2, waiting for PUBCOMP of a PUBREL message sent
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            acknowledge = false;
                                            lock (clientConnection.internalQueue)
                                            {
                                                if (clientConnection.internalQueue.Count > 0)
                                                    msgReceived = (MqttMsgBase)clientConnection.internalQueue.Peek();
                                            }

                                            // it is a PUBCOMP message
                                            if ((msgReceived != null) && (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE))
                                            {
                                                // PUBCOMP message for the current message
                                                if (msgReceived.MessageId == msgInflight.MessageId)
                                                {
                                                    lock (clientConnection.internalQueue)
                                                    {
                                                        // received message processed
                                                        clientConnection.internalQueue.Dequeue();
                                                        acknowledge = true;
                                                        msgReceivedProcessed = true;
                                                    }

                                                    internalEvent = new MsgPublishedInternalEvent(msgReceived, true);
                                                    // notify received acknowledge from broker of a published message
                                                    clientConnection.OnInternalEvent(internalEvent);

                                                    // PUBCOMP received for PUBLISH message with QoS Level 2, remove from session state
                                                    if ((msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) &&
                                                        (clientConnection.Session != null) &&
                                                        (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                                                    {
                                                        clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                                                    }
                                                }
                                            }
                                            // it is a PUBREC message
                                            else if ((msgReceived != null) && (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBREC_TYPE))
                                            {
                                                // another PUBREC message for the current message due to a retransmitted PUBLISH
                                                // I'm in waiting for PUBCOMP, so I can discard clientConnection PUBREC
                                                if (msgReceived.MessageId == msgInflight.MessageId)
                                                {
                                                    lock (clientConnection.internalQueue)
                                                    {
                                                        // received message processed
                                                        clientConnection.internalQueue.Dequeue();
                                                        acknowledge = true;
                                                        msgReceivedProcessed = true;

                                                        // re-enqueue message
                                                        clientConnection.inflightQueue.Enqueue(msgContext);
                                                    }
                                                }
                                            }

                                            // current message not acknowledged
                                            if (!acknowledge)
                                            {
                                                delta = Environment.TickCount - msgContext.Timestamp;
                                                // check timeout for receiving PUBCOMP since PUBREL was sent
                                                if (delta >= clientConnection.Settings.DelayOnRetry)
                                                {
                                                    // max retry not reached, resend
                                                    if (msgContext.Attempt < clientConnection.Settings.AttemptsOnRetry)
                                                    {
                                                        msgContext.State = MqttMsgState.SendPubrel;

                                                        // re-enqueue message
                                                        clientConnection.inflightQueue.Enqueue(msgContext);

                                                        // update timeout (0 -> reanalyze queue immediately)
                                                        timeout = 0;
                                                    }
                                                    else
                                                    {
                                                        // PUBCOMP not received, PUBREL retries failed, need to remove from session inflight messages too
                                                        if ((clientConnection.Session != null) &&
                                                            (clientConnection.Session.InflightMessages.ContainsKey(msgContext.Key)))
                                                        {
                                                            clientConnection.Session.InflightMessages.Remove(msgContext.Key);
                                                        }

                                                        // if PUBCOMP for a PUBLISH message not received after retries, raise event for not published
                                                        internalEvent = new MsgPublishedInternalEvent(msgInflight, false);
                                                        // notify not received acknowledge from broker and message not published
                                                        clientConnection.OnInternalEvent(internalEvent);
                                                    }
                                                }
                                                else
                                                {
                                                    // re-enqueue message
                                                    clientConnection.inflightQueue.Enqueue(msgContext);

                                                    // update timeout
                                                    int msgTimeout = (clientConnection.Settings.DelayOnRetry - delta);
                                                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                                                }
                                            }
                                        }
                                        break;

                                    case MqttMsgState.SendPubrec:

                                        // TODO : impossible ? --> QueuedQos2 ToAcknowledge
                                        break;

                                    case MqttMsgState.SendPubrel:

                                        // QoS 2, PUBREL message to send to broker, state change to wait PUBCOMP
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            MqttMsgPubrel pubrel = new MqttMsgPubrel();
                                            pubrel.MessageId = msgInflight.MessageId;

                                            msgContext.State = MqttMsgState.WaitForPubcomp;
                                            msgContext.Timestamp = Environment.TickCount;
                                            msgContext.Attempt++;
                                            // retry ? set dup flag [v3.1.1] no needed
                                            if (clientConnection.ProtocolVersion == MqttProtocolVersion.Version_3_1)
                                            {
                                                if (msgContext.Attempt > 1)
                                                    pubrel.DupFlag = true;
                                            }

                                            clientConnection.Send(pubrel);

                                            // update timeout : minimum between delay (based on current message sent) or current timeout
                                            timeout = (clientConnection.Settings.DelayOnRetry < timeout) ? clientConnection.Settings.DelayOnRetry : timeout;

                                            // re-enqueue message
                                            clientConnection.inflightQueue.Enqueue(msgContext);
                                        }
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
                            if (timeout == Int32.MaxValue)
                                timeout = Timeout.Infinite;

                            // if message received is orphan, no corresponding message in inflight queue
                            // based on messageId, we need to remove from the queue
                            if ((msgReceived != null) && !msgReceivedProcessed)
                            {
                                clientConnection.internalQueue.Dequeue();
                            }
                        }
                    }
                }
            }
            catch (MqttCommunicationException e)
            {
                // possible exception on Send, I need to re-enqueue not sent message
                if (msgContext != null)
                    // re-enqueue message
                    clientConnection.inflightQueue.Enqueue(msgContext);



                // raise disconnection client event
                clientConnection.OnConnectionClosing();
            }
        }
    }
}