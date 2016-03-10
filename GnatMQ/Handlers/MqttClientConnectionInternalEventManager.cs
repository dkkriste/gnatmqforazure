namespace GnatMQForAzure.Handlers
{
    using System;

    using GnatMQForAzure.Events;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Managers;
    using GnatMQForAzure.Messages;

    public class MqttClientConnectionInternalEventManager
    {
        private readonly MqttPublishManager publishManager;

        private readonly MqttSubscriberManager subscriberManager;

        private readonly MqttOutgoingMessageManager outgoingMessageManager;

        public MqttClientConnectionInternalEventManager(MqttPublishManager publishManager, MqttSubscriberManager subscriberManager, MqttOutgoingMessageManager outgoingMessageManager)
        {
            this.publishManager = publishManager;
            this.subscriberManager = subscriberManager;
            this.outgoingMessageManager = outgoingMessageManager;
        }

        public void ProcessInternalEventQueue(MqttClientConnection clientConnection)
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
                            OnMqttMsgSubscribeReceived(clientConnection, subscribe.MessageId, subscribe.Topics, subscribe.QoSLevels);
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
                                OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                            }

                            break;

                        // (PUBACK received for QoS Level 1)
                        case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:
                            OnMqttMsgPublished(clientConnection, msg.MessageId, true);
                            break;

                        // (PUBREL received for QoS Level 2)
                        case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:
                            OnMqttMsgPublishReceived((MqttMsgPublish)msg);
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
            if ((clientConnection.eventQueue.Count == 0) && clientConnection.isConnectionClosing)
            {
                // client raw disconnection
                clientConnection.OnConnectionClosed();
            }
        }

        private void OnMqttMsgPublishReceived(MqttMsgPublish msg)
        {
            // create PUBLISH message to publish
            // [v3.1.1] DUP flag from an incoming PUBLISH message is not propagated to subscribers
            //          It should be set in the outgoing PUBLISH message based on transmission for each subscriber
            MqttMsgPublish publish = new MqttMsgPublish(msg.Topic, msg.Message, false, msg.QosLevel, msg.Retain);

            // publish message through publisher manager
            this.publishManager.Publish(publish);
        }

        private void OnMqttMsgSubscribeReceived(MqttClientConnection clientConnection, ushort messageId, string[] topics, byte[] qosLevels)
        {
            for (int i = 0; i < topics.Length; i++)
            {
                // TODO : business logic to grant QoS levels based on some conditions ?
                //        now the broker granted the QoS levels requested by client

                // subscribe client for each topic and QoS level requested
                this.subscriberManager.Subscribe(topics[i], qosLevels[i], clientConnection);
            }
            
            // send SUBACK message to the client
            outgoingMessageManager.Suback(clientConnection, messageId, qosLevels);

            for (int i = 0; i < topics.Length; i++)
            {
                // publish retained message on the current subscription
                this.publishManager.PublishRetaind(topics[i], clientConnection.ClientId);
            }
        }

        private void OnMqttMsgUnsubscribeReceived(MqttClientConnection clientConnection, ushort messageId, string[] topics)
        {
            for (int i = 0; i < topics.Length; i++)
            {
                // unsubscribe client for each topic requested
                this.subscriberManager.Unsubscribe(topics[i], clientConnection);
            }

            // send UNSUBACK message to the client
            outgoingMessageManager.Unsuback(clientConnection, messageId);
        }

        private void OnMqttMsgPublished(MqttClientConnection clientConnection, ushort messageId, bool isPublished)
        {
        }

        private void OnMqttMsgSubscribed(MqttClientConnection clientConnection, MqttMsgSuback suback)
        {
        }

        private void OnMqttMsgUnsubscribed(MqttClientConnection clientConnection, ushort messageId)
        {
        }

        private void OnMqttMsgDisconnected(MqttClientConnection clientConnection)
        {
            // close the client
            clientConnection.OnConnectionClosed();
        }
    }
}