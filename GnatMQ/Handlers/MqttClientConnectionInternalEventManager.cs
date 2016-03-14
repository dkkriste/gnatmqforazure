namespace GnatMQForAzure.Handlers
{
    using GnatMQForAzure.Events;
    using GnatMQForAzure.Managers;
    using GnatMQForAzure.Messages;

    public static class MqttClientConnectionInternalEventManager
    {
        public static void ProcessInternalEventQueue(MqttClientConnection clientConnection)
        {
            if (!clientConnection.IsRunning)
            {
                return;
            }

            InternalEvent internalEvent;
            if (clientConnection.EventQueue.TryDequeue(out internalEvent))
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
                            OnMqttMsgSubscribed(clientConnection, (MqttMsgSuback)msg);
                            break;

                        // PUBLISH message received
                        case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:
                            // PUBLISH message received in a published internal event, no publish succeeded
                            if (internalEvent.GetType() == typeof(MsgPublishedInternalEvent))
                            {
                                OnMqttMsgPublished(clientConnection, msg.MessageId, false);
                            }
                            else
                            {
                                // raise PUBLISH message received event 
                                clientConnection.OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                            }

                            break;

                        // (PUBACK received for QoS Level 1)
                        case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:
                            OnMqttMsgPublished(clientConnection, msg.MessageId, true);
                            break;

                        // (PUBREL received for QoS Level 2)
                        case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:
                            clientConnection.OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                            break;

                        // (PUBCOMP received for QoS Level 2)
                        case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:
                            OnMqttMsgPublished(clientConnection, msg.MessageId, true);
                            break;

                        // UNSUBSCRIBE message received from client
                        case MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE:
                            MqttMsgUnsubscribe unsubscribe = (MqttMsgUnsubscribe)msg;
                            OnMqttMsgUnsubscribeReceived(clientConnection, unsubscribe.MessageId, unsubscribe.Topics);
                            break;

                        // UNSUBACK message received
                        case MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE:
                            OnMqttMsgUnsubscribed(clientConnection, msg.MessageId);
                            break;

                        // DISCONNECT message received from client
                        case MqttMsgBase.MQTT_MSG_DISCONNECT_TYPE:
                            OnMqttMsgDisconnected(clientConnection);
                            break;
                    }
                }
            }

            // all events for received messages dispatched, check if there is closing connection
            if ((clientConnection.EventQueue.Count == 0) && clientConnection.IsConnectionClosing)
            {
                // client raw disconnection
                clientConnection.OnConnectionClosed();
            }
        }

        private static void OnMqttMsgUnsubscribeReceived(MqttClientConnection clientConnection, ushort messageId, string[] topics)
        {
            for (int i = 0; i < topics.Length; i++)
            {
                // unsubscribe client for each topic requested
                MqttSubscriberManager.Unsubscribe(topics[i], clientConnection);
            }

            // send UNSUBACK message to the client
            MqttOutgoingMessageManager.Unsuback(clientConnection, messageId);
        }

        private static void OnMqttMsgPublished(MqttClientConnection clientConnection, ushort messageId, bool isPublished)
        {
        }

        private static void OnMqttMsgSubscribed(MqttClientConnection clientConnection, MqttMsgSuback suback)
        {
        }

        private static void OnMqttMsgUnsubscribed(MqttClientConnection clientConnection, ushort messageId)
        {
        }

        private static void OnMqttMsgDisconnected(MqttClientConnection clientConnection)
        {
            // close the client
            clientConnection.OnConnectionClosed();
        }
    }
}