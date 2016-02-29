namespace GnatMQForAzure.Managers
{
    using GnatMQForAzure.Events;
    using GnatMQForAzure.Messages;

    public class MqttClientConnectionInternalEventManager
    {
        /// <summary>
        /// Thread for raising event
        /// </summary>
        private void DispatchEventThread(MqttClientConnection clientConnection)
        {
            while (clientConnection.isRunning)
            {
                if ((clientConnection.eventQueue.Count == 0) && !clientConnection.isConnectionClosing)
                {
                    // broker need to receive the first message (CONNECT)
                    // within a reasonable amount of time after TCP/IP connection
                    if (!clientConnection.IsConnected)
                    {
                        // wait on receiving message from client with a connection timeout
                        if (!clientConnection.receiveEventWaitHandle.WaitOne(clientConnection.Settings.TimeoutOnConnection))
                        {
                            // client must close connection
                            clientConnection.Close();

                            // client raw disconnection
                            clientConnection.OnConnectionClosed();
                        }
                    }
                    else
                    {
                        // wait on receiving message from client
                        clientConnection.receiveEventWaitHandle.WaitOne();
                    }
                }

                // check if it is running or we are closing client
                if (clientConnection.isRunning)
                {
                    // get event from queue
                    InternalEvent internalEvent = null;
                    lock (clientConnection.eventQueue)
                    {
                        if (clientConnection.eventQueue.Count > 0)
                            internalEvent = (InternalEvent)clientConnection.eventQueue.Dequeue();
                    }

                    // it's an event with a message inside
                    if (internalEvent != null)
                    {
                        MqttMsgBase msg = ((MsgInternalEvent)internalEvent).Message;

                        if (msg != null)
                        {
                            switch (msg.Type)
                            {
                                // CONNECT message received
                                case MqttMsgBase.MQTT_MSG_CONNECT_TYPE:

                                    // raise connected client event (CONNECT message received)
                                    clientConnection.OnMqttMsgConnected((MqttMsgConnect)msg);
                                    break;


                                // SUBSCRIBE message received
                                case MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE:

                                    MqttMsgSubscribe subscribe = (MqttMsgSubscribe)msg;
                                    // raise subscribe topic event (SUBSCRIBE message received)
                                    clientConnection.OnMqttMsgSubscribeReceived(subscribe.MessageId, subscribe.Topics, subscribe.QoSLevels);
                                    break;


                                // SUBACK message received
                                case MqttMsgBase.MQTT_MSG_SUBACK_TYPE:

                                    // raise subscribed topic event (SUBACK message received)
                                    clientConnection.OnMqttMsgSubscribed((MqttMsgSuback)msg);
                                    break;

                                // PUBLISH message received
                                case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:

                                    // PUBLISH message received in a published internal event, no publish succeeded
                                    if (internalEvent.GetType() == typeof(MsgPublishedInternalEvent))
                                        clientConnection.OnMqttMsgPublished(msg.MessageId, false);
                                    else
                                        // raise PUBLISH message received event 
                                        clientConnection.OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                                    break;

                                // PUBACK message received
                                case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:

                                    // raise published message event
                                    // (PUBACK received for QoS Level 1)
                                    clientConnection.OnMqttMsgPublished(msg.MessageId, true);
                                    break;

                                // PUBREL message received
                                case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:

                                    // raise message received event 
                                    // (PUBREL received for QoS Level 2)
                                    clientConnection.OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                                    break;

                                // PUBCOMP message received
                                case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:

                                    // raise published message event
                                    // (PUBCOMP received for QoS Level 2)
                                    clientConnection.OnMqttMsgPublished(msg.MessageId, true);
                                    break;

                                // UNSUBSCRIBE message received from client
                                case MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE:

                                    MqttMsgUnsubscribe unsubscribe = (MqttMsgUnsubscribe)msg;
                                    // raise unsubscribe topic event (UNSUBSCRIBE message received)
                                    clientConnection.OnMqttMsgUnsubscribeReceived(unsubscribe.MessageId, unsubscribe.Topics);
                                    break;


                                // UNSUBACK message received
                                case MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE:

                                    // raise unsubscribed topic event
                                    clientConnection.OnMqttMsgUnsubscribed(msg.MessageId);
                                    break;

                                // DISCONNECT message received from client
                                case MqttMsgDisconnect.MQTT_MSG_DISCONNECT_TYPE:

                                    // raise disconnected client event (DISCONNECT message received)
                                    clientConnection.OnMqttMsgDisconnected();
                                    break;

                            }
                        }
                    }

                    // all events for received messages dispatched, check if there is closing connection
                    if ((clientConnection.eventQueue.Count == 0) && clientConnection.isConnectionClosing)
                    {
                        // client must close connection
                        clientConnection.Close();

                        // client raw disconnection
                        clientConnection.OnConnectionClosed();
                    }
                }
            }
        }
    }
}