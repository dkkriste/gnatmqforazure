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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;

    using GnatMQForAzure.Handlers;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Session;
    using GnatMQForAzure.Utility;

    /// <summary>
    /// Manager for publishing messages
    /// </summary>
    public class MqttPublishManager
    {
        private readonly MqttClientConnectionIncomingMessageManager incomingMessageManager;

        // queue messages to publish
        private readonly BlockingCollection<MqttMsgBase> publishQueue;

        // reference to subscriber manager
        private readonly MqttSubscriberManager subscriberManager;

        // reference to session manager
        private readonly MqttSessionManager sessionManager;

        // retained messages
        private readonly ConcurrentDictionary<string, MqttMsgPublish> retainedMessages;

        // subscriptions to send retained messages (new subscriber or reconnected client)
        private readonly BlockingCollection<MqttSubscription> subscribersForRetained;

        // client id to send outgoing session messages
        private readonly BlockingCollection<string> clientsForSession;

        private bool isRunning;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="subscriberManager">Reference to subscriber manager</param>
        /// <param name="sessionManager">Reference to session manager</param>
        public MqttPublishManager(MqttSubscriberManager subscriberManager, MqttSessionManager sessionManager)
        {
            // save reference to subscriber manager
            this.subscriberManager = subscriberManager;

            // save reference to session manager
            this.sessionManager = sessionManager;

            // create empty list for retained messages
            this.retainedMessages = new ConcurrentDictionary<string, MqttMsgPublish>();

            // create empty list for destination subscribers for retained message
            this.subscribersForRetained = new BlockingCollection<MqttSubscription>();

            // create empty list for destination client for outgoing session message
            this.clientsForSession = new BlockingCollection<string>();

            // create publish messages queue
            this.publishQueue = new BlockingCollection<MqttMsgBase>();
        }
        
        /// <summary>
        /// Start publish handling
        /// </summary>
        public void Start()
        {
            this.isRunning = true;
            Fx.StartThread(this.PublishThread);
            Fx.StartThread(this.ClientsForSessionThread);
            Fx.StartThread(this.SubscribersForRetainedThread);
        }

        /// <summary>
        /// Stop publish handling
        /// </summary>
        public void Stop()
        {
            this.isRunning = false;
        }

        /// <summary>
        /// Publish message
        /// </summary>
        /// <param name="publish">Message to publish</param>
        public void Publish(MqttMsgPublish publish)
        {
            if (publish.Retain)
            {
                // retained message already exists for the topic
                if (retainedMessages.ContainsKey(publish.Topic))
                {
                    // if empty message, remove current retained message
                    if (publish.Message.Length == 0)
                    {
                        MqttMsgPublish oldRetained;
                        retainedMessages.TryRemove(publish.Topic, out oldRetained);
                    }
                    else
                    {
                        // set new retained message for the topic
                        retainedMessages[publish.Topic] = publish;
                    }
                }
                else
                {
                    // add new topic with related retained message
                    retainedMessages.TryAdd(publish.Topic, publish);
                }
            }

            // enqueue
            this.publishQueue.Add(publish);
        }

        /// <summary>
        /// Publish retained message for a topic to a client
        /// </summary>
        /// <param name="topic">Topic to search for a retained message</param>
        /// <param name="clientId">Client Id to send retained message</param>
        public void PublishRetaind(string topic, string clientId)
        {
            MqttSubscription subscription = this.subscriberManager.GetSubscription(topic, clientId);

            // add subscription to list of subscribers for receiving retained messages
            if (subscription != null)
            {
                this.subscribersForRetained.Add(subscription);
            }
        }

        /// <summary>
        /// Publish outgoing session messages for a client
        /// </summary>
        /// <param name="clientId">Client Id to send outgoing session messages</param>
        public void PublishSession(string clientId)
        {
            this.clientsForSession.Add(clientId);
        }

        private void SubscribersForRetainedThread()
        {
            while (isRunning)
            {
                try
                {
                    var subscription = this.subscribersForRetained.Take();
                    var query = from p in this.retainedMessages
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
                            incomingMessageManager.Publish(
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

        private void ClientsForSessionThread()
        {
            while (isRunning)
            {
                try
                {
                    string clientId = this.clientsForSession.Take();

                    MqttBrokerSession session = this.sessionManager.GetSession(clientId);

                    while (session.OutgoingMessages.Count > 0)
                    {
                        MqttMsgPublish outgoingMsg = session.OutgoingMessages.Dequeue();

                        var query = from s in session.Subscriptions
                                    where (new Regex(s.Topic)).IsMatch(outgoingMsg.Topic)
                                    // check for topics based also on wildcard with regex
                                    select s;

                        MqttSubscription subscription = query.First();

                        if (subscription != null)
                        {
                            var qosLevel = (subscription.QosLevel < outgoingMsg.QosLevel)
                                                ? subscription.QosLevel
                                                : outgoingMsg.QosLevel;

                            incomingMessageManager.Publish(
                                subscription.ClientConnection,
                                outgoingMsg.Topic,
                                outgoingMsg.Message,
                                qosLevel,
                                outgoingMsg.Retain);
                        }
                    }
                }
                catch (Exception)
                {

                    throw;
                }
            }
        }

        /// <summary>
        /// Process the message queue to publish
        /// </summary>
        private void PublishThread()
        {
            // create event to signal that current thread is ended
            while (this.isRunning)
            {
                var publish = (MqttMsgPublish)this.publishQueue.Take();
                if (publish != null)
                {
                    // get all subscriptions for a topic
                    List<MqttSubscription> subscriptions = this.subscriberManager.GetSubscriptionsByTopic(publish.Topic);

                    byte qosLevel;
                    if ((subscriptions != null) && (subscriptions.Count > 0))
                    {
                        foreach (MqttSubscription subscription in subscriptions)
                        {
                            qosLevel = (subscription.QosLevel < publish.QosLevel)
                                           ? subscription.QosLevel
                                           : publish.QosLevel;

                            // send PUBLISH message to the current subscriber
                            incomingMessageManager.Publish(
                                subscription.ClientConnection,
                                publish.Topic,
                                publish.Message,
                                qosLevel,
                                publish.Retain);
                        }
                    }

                    // get all sessions
                    List<MqttBrokerSession> sessions = this.sessionManager.GetSessions();

                    if ((sessions != null) && (sessions.Count > 0))
                    {
                        foreach (MqttBrokerSession session in sessions)
                        {
                            var query = from s in session.Subscriptions
                                        where (new Regex(s.Topic)).IsMatch(publish.Topic)
                                        select s;

                            MqttSubscriptionComparer comparer =
                                new MqttSubscriptionComparer(
                                    MqttSubscriptionComparer.MqttSubscriptionComparerType.OnClientId);

                            // consider only session active for client disconnected (not online)
                            if (session.ClientConnection == null)
                            {
                                foreach (MqttSubscription subscription in query.Distinct(comparer))
                                {
                                    qosLevel = (subscription.QosLevel < publish.QosLevel)
                                                   ? subscription.QosLevel
                                                   : publish.QosLevel;

                                    // save PUBLISH message for client disconnected (not online)
                                    session.OutgoingMessages.Enqueue(publish);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
