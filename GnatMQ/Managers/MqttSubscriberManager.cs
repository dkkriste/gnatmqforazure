/*
Copyright (c) 2013, 2014 Paolo Patierno

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
and Eclipse Distribution License v1.0 which accompany this distribution. 

The Eclipse Public License is available at 
   http://www.eclipse.org/legal/epl-v10.html
and the Eclipse Distribution License is available at 
   http://www.eclipse.org/org/documents/edl-v10.php.

Contributors:
   Paolo Patierno - initial API and implementation and/or initial documentation
   David Kristensen - optimalization for the azure platform
*/

namespace GnatMQForAzure.Managers
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;

    using GnatMQForAzure.Utility;

    /// <summary>
    /// Manager for topics and subscribers
    /// </summary>
    public class MqttSubscriberManager
    {
        #region Constants ...

        // topic wildcards '+' and '#'
        private const string PLUS_WILDCARD = "+";
        private const string SHARP_WILDCARD = "#";

        // replace for wildcards '+' and '#' for using regular expression on topic match
        private const string PLUS_WILDCARD_REPLACE = @"[^/]+";
        private const string SHARP_WILDCARD_REPLACE = @".*";

        #endregion

        // MQTT subscription comparer
        private readonly MqttSubscriptionComparer comparer;

        // subscribers list for each topic
        private readonly ConcurrentDictionary<string, List<MqttSubscription>> subscribers;

        /// <summary>
        /// Constructor
        /// </summary>
        public MqttSubscriberManager()
        {
            this.subscribers = new ConcurrentDictionary<string, List<MqttSubscription>>();
            this.comparer = new MqttSubscriptionComparer(MqttSubscriptionComparer.MqttSubscriptionComparerType.OnClientId);
        }

        /// <summary>
        /// Add a subscriber for a topic
        /// </summary>
        /// <param name="topic">Topic for subscription</param>
        /// <param name="qosLevel">QoS level for the topic subscription</param>
        /// <param name="clientConnection">Client to subscribe</param>
        public void Subscribe(string topic, byte qosLevel, MqttClientConnection clientConnection)
        {
            string topicReplaced = topic.Replace(PLUS_WILDCARD, PLUS_WILDCARD_REPLACE).Replace(SHARP_WILDCARD, SHARP_WILDCARD_REPLACE);

            var subscriptionsForTopic = subscribers.GetOrAdd(topicReplaced, new List<MqttSubscription>());
            lock (subscriptionsForTopic)
            {
                if (!AlreadySubscribed(clientConnection.ClientId, subscriptionsForTopic))
                {
                    MqttSubscription subscription = new MqttSubscription()
                    {
                        ClientId = clientConnection.ClientId,
                        Topic = topicReplaced,
                        QosLevel = qosLevel,
                        ClientConnection = clientConnection
                    };

                    subscriptionsForTopic.Add(subscription);
                }
            }
        }

        /// <summary>
        /// Remove a subscriber for a topic
        /// </summary>
        /// <param name="topic">Topic for unsubscription</param>
        /// <param name="clientConnection">Client to unsubscribe</param>
        public void Unsubscribe(string topic, MqttClientConnection clientConnection)
        {
            string topicReplaced = topic.Replace(PLUS_WILDCARD, PLUS_WILDCARD_REPLACE).Replace(SHARP_WILDCARD, SHARP_WILDCARD_REPLACE);

            List<MqttSubscription> subscriptionsForTopic;
            if (this.subscribers.TryGetValue(topicReplaced, out subscriptionsForTopic))
            {
                lock (subscriptionsForTopic)
                {
                    foreach (var subscription in subscriptionsForTopic)
                    {
                        if (subscription.ClientId == clientConnection.ClientId)
                        {
                            subscriptionsForTopic.Remove(subscription);
                            subscription.Dispose();
                            // TODO deal with topic with no subscribers
                            return;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Remove a subscriber for all topics
        /// </summary>
        /// <param name="clientConnection">Client to unsubscribe</param>
        public void Unsubscribe(MqttClientConnection clientConnection)
        {
            this.subscribers.Keys.AsParallel().ForAll(c => Unsubscribe(c, clientConnection));
        }

        /// <summary>
        /// Get subscription list for a specified topic and QoS Level
        /// </summary>
        /// <param name="topic">Topic to get subscription list</param>
        /// <param name="qosLevel">QoS level requested</param>
        /// <returns>Subscription list</returns>
        public List<MqttSubscription> GetSubscriptions(string topic, byte qosLevel)
        {
            var query = from ss in this.subscribers
                        where (new Regex(ss.Key)).IsMatch(topic)    // check for topics based also on wildcard with regex
                        from s in this.subscribers[ss.Key]
                        where s.QosLevel == qosLevel                // check for subscriber only with a specified QoS level granted
                        select s;

            // use comparer for multiple subscriptions that overlap (e.g. /test/# and  /test/+/foo)
            // If a client is subscribed to multiple subscriptions with topics that overlap
            // it has more entries into subscriptions list but broker sends only one message
            this.comparer.Type = MqttSubscriptionComparer.MqttSubscriptionComparerType.OnClientId;
            return query.Distinct(comparer).ToList();
        }

        /// <summary>
        /// Get a subscription for a specified topic and client
        /// </summary>
        /// <param name="topic">Topic to get subscription</param>
        /// <param name="clientId">Client Id to get subscription</param>
        /// <returns>Subscription list</returns>
        public MqttSubscription GetSubscription(string topic, string clientId)
        {
            var query = from ss in this.subscribers
                        where (new Regex(ss.Key)).IsMatch(topic)    // check for topics based also on wildcard with regex
                        from s in this.subscribers[ss.Key]
                        where s.ClientId == clientId                // check for subscriber only with a specified Client Id
                        select s;

            // use comparer for multiple subscriptions that overlap (e.g. /test/# and  /test/+/foo)
            // If a client is subscribed to multiple subscriptions with topics that overlap
            // it has more entries into subscriptions list but broker sends only one message
            this.comparer.Type = MqttSubscriptionComparer.MqttSubscriptionComparerType.OnClientId;
            return query.Distinct(comparer).FirstOrDefault();
        }

        /// <summary>
        /// Get subscription list for a specified topic
        /// </summary>
        /// <param name="topic">Topic to get subscription list</param>
        /// <returns>Subscription list</returns>
        public List<MqttSubscription> GetSubscriptionsByTopic(string topic)
        {
            var query = from ss in this.subscribers
                        where (new Regex(ss.Key)).IsMatch(topic)    // check for topics based also on wildcard with regex
                        from s in this.subscribers[ss.Key]
                        select s;

            // use comparer for multiple subscriptions that overlap (e.g. /test/# and  /test/+/foo)
            // If a client is subscribed to multiple subscriptions with topics that overlap
            // it has more entries into subscriptions list but broker sends only one message
            this.comparer.Type = MqttSubscriptionComparer.MqttSubscriptionComparerType.OnClientId;
            return query.Distinct(comparer).ToList();
        }

        /// <summary>
        /// Get subscription list for a specified client
        /// </summary>
        /// <param name="clientId">Client Id to get subscription list</param>
        /// <returns>Subscription lis</returns>
        public List<MqttSubscription> GetSubscriptionsByClient(string clientId)
        {
            var query = from ss in this.subscribers
                        from s in this.subscribers[ss.Key]
                        where s.ClientId == clientId
                        select s;

            // use comparer for multiple subscriptions that overlap (e.g. /test/# and  /test/+/foo)
            // If a client is subscribed to multiple subscriptions with topics that overlap
            // it has more entries into subscriptions list but broker sends only one message
            //this.comparer.Type = MqttSubscriptionComparer.MqttSubscriptionComparerType.OnTopic;
            //return query.Distinct(comparer).ToList();

            // I need all subscriptions, also overlapped (used to save session)
            return query.ToList();
        }

        private bool AlreadySubscribed(string clientId, List<MqttSubscription> currentSubscriptions)
        {
            foreach (var subscription in currentSubscriptions)
            {
                if (subscription.ClientId == clientId)
                {
                    return true;
                }
            }

            return false;
        }
    }
}
