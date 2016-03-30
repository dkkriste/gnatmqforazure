namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Text.RegularExpressions;

    using GnatMQForAzure.Handlers;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Utility;

    public static class RetainedMessageManager
    {
        private static readonly ConcurrentDictionary<string, MqttMsgPublish> RetainedMessages;

        // subscriptions to send retained messages (new subscriber or reconnected client)
        private static readonly BlockingCollection<MqttSubscription> SubscribersForRetained;

        private static bool isRunning;

        static RetainedMessageManager()
        {
            // create empty list for retained messages
            RetainedMessages = new ConcurrentDictionary<string, MqttMsgPublish>();

            // create empty list for destination subscribers for retained message
            SubscribersForRetained = new BlockingCollection<MqttSubscription>();
        }

        public static void Start()
        {
            isRunning = true;
            Fx.StartThread(SubscribersForRetainedThread);
        }

        public static void Stop()
        {
            isRunning = false;
        }

        public static void CheckForAndSetRetainedMessage(MqttMsgPublish publish)
        {
            if (publish.Retain)
            {
                // retained message already exists for the topic
                if (RetainedMessages.ContainsKey(publish.Topic))
                {
                    // if empty message, remove current retained message
                    if (publish.Message.Length == 0)
                    {
                        MqttMsgPublish oldRetained;
                        RetainedMessages.TryRemove(publish.Topic, out oldRetained);
                    }
                    else
                    {
                        // set new retained message for the topic
                        RetainedMessages[publish.Topic] = publish;
                    }
                }
                else
                {
                    // add new topic with related retained message
                    RetainedMessages.TryAdd(publish.Topic, publish);
                }
            }
        }

        /// <summary>
        /// Publish retained message for a topic to a client
        /// </summary>
        /// <param name="topic">Topic to search for a retained message</param>
        /// <param name="clientId">Client Id to send retained message</param>
        public static void PublishRetaind(string topic, MqttClientConnection clientConnection)
        {
            MqttSubscription subscription = MqttSubscriberManager.GetSubscription(topic, clientConnection);

            // add subscription to list of subscribers for receiving retained messages
            if (subscription != null)
            {
                SubscribersForRetained.Add(subscription);
            }
        }

        private static void SubscribersForRetainedThread()
        {
            while (isRunning)
            {
                try
                {
                    var subscription = SubscribersForRetained.Take();
                    var query = from p in RetainedMessages
                                where (new Regex(subscription.Topic)).IsMatch(p.Key)
                                // check for topics based also on wildcard with regex
                                select p.Value;

                    if (query.Any())
                    {
                        foreach (MqttMsgPublish retained in query)
                        {
                            var qosLevel = (subscription.QosLevel < retained.QosLevel)
                                                ? subscription.QosLevel
                                                : retained.QosLevel;

                            // send PUBLISH message to the current subscriber
                            MqttMessageToClientConnectionManager.Publish(
                                subscription.ClientConnection,
                                retained.Topic,
                                retained.Message,
                                qosLevel,
                                retained.Retain);
                        }
                    }
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }
    }
}