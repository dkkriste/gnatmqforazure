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

    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Utility;

    /// <summary>
    /// Manager for topics and subscribers
    /// </summary>
    public static class MqttSubscriberManager
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
        private static readonly MqttSubscriptionComparer Comparer;

        private static readonly ConcurrentDictionary<string, List<MqttSubscription>> NonWildcardSubscriptions;

        private static readonly ConcurrentDictionary<string, List<MqttSubscription>> WildcardSubscriptions; 

        static MqttSubscriberManager()
        {
            NonWildcardSubscriptions = new ConcurrentDictionary<string, List<MqttSubscription>>();
            WildcardSubscriptions = new ConcurrentDictionary<string, List<MqttSubscription>>();
            Comparer = new MqttSubscriptionComparer(MqttSubscriptionComparer.MqttSubscriptionComparerType.OnClientId);
        }

        /// <summary>
        /// Add a subscriber for a topic
        /// </summary>
        /// <param name="topic">Topic for subscription</param>
        /// <param name="qosLevel">QoS level for the topic subscription</param>
        /// <param name="clientConnection">Client to subscribe</param>
        public static void Subscribe(string topic, byte qosLevel, MqttClientConnection clientConnection)
        {
            if (IsWildcardSubscription(topic))
            {
                SubscribeWithWildcard(topic, qosLevel, clientConnection);
            }
            else
            {
                SubscribeWithoutWildcard(topic, qosLevel, clientConnection);
            }
        }

        /// <summary>
        /// Remove a subscriber for a topic
        /// </summary>
        /// <param name="topic">Topic for unsubscription</param>
        /// <param name="clientConnection">Client to unsubscribe</param>
        public static void Unsubscribe(string topic, MqttClientConnection clientConnection)
        {
            if (IsWildcardSubscription(topic))
            {
                UnsubscribeFromTopicWithWildcard(topic, clientConnection);
            }
            else
            {
                UnsubscribeFromTopicWithoutWildcard(topic, clientConnection);
            }
        }

        public static MqttSubscription GetSubscription(string topic, MqttClientConnection clientConnection)
        {
            MqttSubscription subscription;
            if (IsWildcardSubscription(topic))
            {
                var topicReplaced = topic.Replace(PLUS_WILDCARD, PLUS_WILDCARD_REPLACE).Replace(SHARP_WILDCARD, SHARP_WILDCARD_REPLACE);
                clientConnection.Subscriptions.TryGetValue(topicReplaced, out subscription);
            }
            else
            {
                clientConnection.Subscriptions.TryGetValue(topic, out subscription);
            }

            return subscription;
        }

        /// <summary>
        /// Get subscription list for a specified topic
        /// </summary>
        /// <param name="topic">Topic to get subscription list</param>
        /// <returns>Subscription list</returns>
        public static List<MqttSubscription> GetSubscriptionsByTopic(string topic)
        {
            List<MqttSubscription> allSubscriptionsMatchingTopic = new List<MqttSubscription>();
            List<MqttSubscription> nonWildcardSubscriptionsWithTopic;
            if (NonWildcardSubscriptions.TryGetValue(topic, out nonWildcardSubscriptionsWithTopic))
            {
                allSubscriptionsMatchingTopic.AddRange(nonWildcardSubscriptionsWithTopic);
            }
            
            foreach (var wildcardTopic in WildcardSubscriptions.Keys)
            {
                List<MqttSubscription> subscriptions; 
                if (new Regex(wildcardTopic).IsMatch(topic) && WildcardSubscriptions.TryGetValue(wildcardTopic, out subscriptions))
                {
                    allSubscriptionsMatchingTopic.AddRange(subscriptions);
                }
            }

            // use comparer for multiple subscriptions that overlap (e.g. /test/# and  /test/+/foo)
            // If a client is subscribed to multiple subscriptions with topics that overlap
            // it has more entries into subscriptions list but broker sends only one message
            Comparer.Type = MqttSubscriptionComparer.MqttSubscriptionComparerType.OnClientId;
            return allSubscriptionsMatchingTopic.Distinct(Comparer).ToList();
        }

        private static void SubscribeWithWildcard(string topic, byte qosLevel, MqttClientConnection clientConnection)
        {
            string topicReplaced = topic.Replace(PLUS_WILDCARD, PLUS_WILDCARD_REPLACE).Replace(SHARP_WILDCARD, SHARP_WILDCARD_REPLACE);

            var subscriptionsForTopic = WildcardSubscriptions.GetOrAdd(topicReplaced, new List<MqttSubscription>());
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

                    clientConnection.Subscriptions.TryAdd(topicReplaced, subscription);
                }
            }
        }

        private static void SubscribeWithoutWildcard(string topic, byte qosLevel, MqttClientConnection clientConnection)
        {
            var subscriptionsForTopic = NonWildcardSubscriptions.GetOrAdd(topic, new List<MqttSubscription>());
            lock (subscriptionsForTopic)
            {
                if (!AlreadySubscribed(clientConnection.ClientId, subscriptionsForTopic))
                {
                    MqttSubscription subscription = new MqttSubscription()
                    {
                        ClientId = clientConnection.ClientId,
                        Topic = topic,
                        QosLevel = qosLevel,
                        ClientConnection = clientConnection
                    };

                    subscriptionsForTopic.Add(subscription);

                    clientConnection.Subscriptions.TryAdd(topic, subscription);
                }
            }
        }

        private static void UnsubscribeFromTopicWithWildcard(string topic, MqttClientConnection clientConnection)
        {
            string topicReplaced = topic.Replace(PLUS_WILDCARD, PLUS_WILDCARD_REPLACE).Replace(SHARP_WILDCARD, SHARP_WILDCARD_REPLACE);

            List<MqttSubscription> subscriptionsForTopic;
            if (WildcardSubscriptions.TryGetValue(topicReplaced, out subscriptionsForTopic))
            {
                lock (subscriptionsForTopic)
                {
                    foreach (var subscription in subscriptionsForTopic)
                    {
                        if (subscription.ClientId == clientConnection.ClientId)
                        {
                            subscriptionsForTopic.Remove(subscription);
                            // TODO deal with topic with no subscribers

                            MqttSubscription subscriptionToBeRemoved;
                            clientConnection.Subscriptions.TryRemove(topicReplaced, out subscriptionToBeRemoved);
                            return;
                        }
                    }
                }
            }
        }

        private static void UnsubscribeFromTopicWithoutWildcard(string topic, MqttClientConnection clientConnection)
        {
            List<MqttSubscription> subscriptionsForTopic;
            if (NonWildcardSubscriptions.TryGetValue(topic, out subscriptionsForTopic))
            {
                lock (subscriptionsForTopic)
                {
                    foreach (var subscription in subscriptionsForTopic)
                    {
                        if (subscription.ClientId == clientConnection.ClientId)
                        {
                            subscriptionsForTopic.Remove(subscription);
                            // TODO deal with topic with no subscribers

                            MqttSubscription subscriptionToBeRemoved;
                            clientConnection.Subscriptions.TryRemove(topic, out subscriptionToBeRemoved);
                            return;
                        }
                    }
                }
            }
        }

        private static bool AlreadySubscribed(string clientId, List<MqttSubscription> currentSubscriptions)
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

        private static bool IsWildcardSubscription(string topic)
        {
            return topic.Contains(PLUS_WILDCARD) || topic.Contains(SHARP_WILDCARD);
        }
    }
}
