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
    using System.Threading;

    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Handlers;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Session;
    using GnatMQForAzure.Utility;

    public class MqttPublishManager : IMqttRunnable, IPeriodicallyLoggable
    {
        private readonly ILogger logger;

        private readonly BlockingCollection<MqttMsgBase> publishQueue;

        private readonly BlockingCollection<MqttMsgBase> sessionPublishQueue;

        // client id to send outgoing session messages
        private readonly BlockingCollection<string> clientsForSession;

        private bool isRunning;

        private int numberOfMessagesPublished;

        /// <summary>
        /// Constructor
        /// </summary>
        public MqttPublishManager(ILogger logger)
        {
            this.logger = logger;

            // create empty list for destination client for outgoing session message
            this.clientsForSession = new BlockingCollection<string>();

            // create publish messages queue
            this.publishQueue = new BlockingCollection<MqttMsgBase>();

            this.sessionPublishQueue = new BlockingCollection<MqttMsgBase>();
        }
        
        public void Start()
        {
            isRunning = true;
            Fx.StartThread(PublishThread);
            Fx.StartThread(ClientsForSessionThread);
            Fx.StartThread(SessionPublishThread);
        }

        public void Stop()
        {
            isRunning = false;
        }

        public void PeriodicLogging()
        {
            logger.LogMetric(this, LoggerConstants.PublishQueueSize, publishQueue.Count);

            var messagesPublishedCopy = numberOfMessagesPublished;
            logger.LogMetric(this, LoggerConstants.NumberOfMessagesPublished, messagesPublishedCopy);

            Interlocked.Add(ref numberOfMessagesPublished, -messagesPublishedCopy);
        }

        /// <summary>
        /// Publish message
        /// </summary>
        /// <param name="publish">Message to publish</param>
        public void Publish(MqttMsgPublish publish)
        {
            RetainedMessageManager.CheckForAndSetRetainedMessage(publish);

            // enqueue
            this.publishQueue.Add(publish);
            this.sessionPublishQueue.Add(publish);
        }

        /// <summary>
        /// Publish outgoing session messages for a client
        /// </summary>
        /// <param name="clientId">Client Id to send outgoing session messages</param>
        public void PublishSession(string clientId)
        {
            this.clientsForSession.Add(clientId);
        }

        #region Worker threads

        private void ClientsForSessionThread()
        {
            while (isRunning)
            {
                try
                {
                    string clientId = this.clientsForSession.Take();

                    MqttBrokerSession session = MqttSessionManager.GetSession(clientId);

                    while (session.OutgoingMessages.Count > 0)
                    {
                        MqttMsgPublish outgoingMsg = session.OutgoingMessages.Dequeue();

                        var query = from s in session.Subscriptions
                                    where (new Regex(s.Topic)).IsMatch(outgoingMsg.Topic)
                                    // check for topics based also on wildcard with regex
                                    select s;

                        MqttSubscription subscription = query.FirstOrDefault();

                        if (subscription != null)
                        {
                            var qosLevel = (subscription.QosLevel < outgoingMsg.QosLevel) ? subscription.QosLevel : outgoingMsg.QosLevel;
                            MqttMessageToClientConnectionManager.Publish(subscription.ClientConnection, outgoingMsg.Topic, outgoingMsg.Message, qosLevel,outgoingMsg.Retain);
                        }
                    }
                }
                catch (Exception exception)
                {
                    logger.LogException(this, exception);
                }
            }
        }

        private void PublishThread()
        {
            while (this.isRunning)
            {
                try
                {
                    var publish = (MqttMsgPublish)this.publishQueue.Take();
                    if (publish != null)
                    {
                        Interlocked.Increment(ref numberOfMessagesPublished);

                        // get all subscriptions for a topic
                        var subscriptions = MqttSubscriberManager.GetSubscriptionsByTopic(publish.Topic);
                        foreach (var subscription in subscriptions)
                        {
                            var qosLevel = (subscription.QosLevel < publish.QosLevel) ? subscription.QosLevel : publish.QosLevel;

                            // send PUBLISH message to the current subscriber
                            MqttMessageToClientConnectionManager.Publish(subscription.ClientConnection, publish.Topic, publish.Message, qosLevel, publish.Retain);
                        }
                    }
                }
                catch (Exception exception)
                {
                    logger.LogException(this, exception);
                }
            }
        }

        private void SessionPublishThread()
        {
            while (this.isRunning)
            {
                try
                {
                    var publish = (MqttMsgPublish)this.sessionPublishQueue.Take();
                    if (publish != null)
                    {
                        // get all sessions
                        List<MqttBrokerSession> sessions = MqttSessionManager.GetSessions();

                        if ((sessions != null) && (sessions.Count > 0))
                        {
                            foreach (MqttBrokerSession session in sessions)
                            {
                                var query = from s in session.Subscriptions
                                            where (new Regex(s.Topic)).IsMatch(publish.Topic)
                                            select s;

                                MqttSubscriptionComparer comparer = new MqttSubscriptionComparer(MqttSubscriptionComparer.MqttSubscriptionComparerType.OnClientId);

                                // consider only session active for client disconnected (not online)
                                if (session.ClientConnection == null)
                                {
                                    foreach (MqttSubscription subscription in query.Distinct(comparer))
                                    {
                                        // save PUBLISH message for client disconnected (not online)
                                        session.OutgoingMessages.Enqueue(publish);
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception exception)
                {
                    logger.LogException(this, exception);
                }
            }
        }

        #endregion
    }
}
