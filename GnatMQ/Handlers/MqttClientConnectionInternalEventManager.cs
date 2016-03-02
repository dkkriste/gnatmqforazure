namespace GnatMQForAzure.Handlers
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
            if (!clientConnection.isRunning)
            {
                return;
            }

            InternalEvent internalEvent;
            if (clientConnection.eventQueue.TryDequeue(out internalEvent))
            {
                MqttMsgBase msg = ((MsgInternalEvent)internalEvent).Message;
                if (msg != null)
                {
                    switch (msg.Type)
                    {
                        // CONNECT message received
                        case MqttMsgBase.MQTT_MSG_CONNECT_TYPE:
                            clientConnection.OnMqttMsgConnected((MqttMsgConnect)msg);
                            break;

                        // SUBSCRIBE message received
                        case MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE:
                            MqttMsgSubscribe subscribe = (MqttMsgSubscribe)msg;
                            clientConnection.OnMqttMsgSubscribeReceived(subscribe.MessageId, subscribe.Topics, subscribe.QoSLevels);
                            break;

                        // SUBACK message received
                        case MqttMsgBase.MQTT_MSG_SUBACK_TYPE:
                            clientConnection.OnMqttMsgSubscribed((MqttMsgSuback)msg);
                            break;

                        // PUBLISH message received
                        case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:
                            // PUBLISH message received in a published internal event, no publish succeeded
                            if (internalEvent.GetType() == typeof(MsgPublishedInternalEvent))
                            {
                                clientConnection.OnMqttMsgPublished(msg.MessageId, false);
                            }
                            else
                            {
                                // raise PUBLISH message received event 
                                clientConnection.OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                            }

                            break;

                        // PUBACK message received
                        case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:
                            // (PUBACK received for QoS Level 1)
                            clientConnection.OnMqttMsgPublished(msg.MessageId, true);
                            break;

                        // PUBREL message received
                        case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:
                            // (PUBREL received for QoS Level 2)
                            clientConnection.OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                            break;

                        // PUBCOMP message received
                        case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:
                            // (PUBCOMP received for QoS Level 2)
                            clientConnection.OnMqttMsgPublished(msg.MessageId, true);
                            break;

                        // UNSUBSCRIBE message received from client
                        case MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE:
                            MqttMsgUnsubscribe unsubscribe = (MqttMsgUnsubscribe)msg;
                            clientConnection.OnMqttMsgUnsubscribeReceived(unsubscribe.MessageId, unsubscribe.Topics);
                            break;

                        // UNSUBACK message received
                        case MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE:
                            clientConnection.OnMqttMsgUnsubscribed(msg.MessageId);
                            break;

                        // DISCONNECT message received from client
                        case MqttMsgBase.MQTT_MSG_DISCONNECT_TYPE:
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