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
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Session;

    /// <summary>
    /// Manager for client session
    /// </summary>
    public static class MqttSessionManager
    {
        // subscription info for each client
        private static readonly ConcurrentDictionary<string, MqttBrokerSession> Sessions;

        static MqttSessionManager()
        {
            Sessions = new ConcurrentDictionary<string, MqttBrokerSession>();
        }

        /// <summary>
        /// Save session for a client (all related subscriptions)
        /// </summary>
        /// <param name="clientId">Client Id to save subscriptions</param>
        /// <param name="clientSession">Client session with inflight messages</param>
        /// <param name="subscriptions">Subscriptions to save</param>
        public static void SaveSession(string clientId, MqttClientSession clientSession, List<MqttSubscription> subscriptions)
        {
            MqttBrokerSession session = null;

            // session doesn't exist
            if (!Sessions.ContainsKey(clientId))
            {
                // create new session
                session = new MqttBrokerSession();
                session.ClientId = clientId;

                // add to sessions list
                Sessions.TryAdd(clientId, session);
            }
            else
            {
                // get existing session
                session = Sessions[clientId];
            }

            // null reference to disconnected client
            session.ClientConnection = null;

            // update subscriptions
            session.Subscriptions = new List<MqttSubscription>();
            foreach (MqttSubscription subscription in subscriptions)
            {
                session.Subscriptions.Add(new MqttSubscription(subscription.ClientId, subscription.Topic, subscription.QosLevel, null));
            }
            
            // update inflight messages
            session.InflightMessages = new Hashtable();
            foreach (MqttMsgContext msgContext in clientSession.InflightMessages.Values)
            {
                session.InflightMessages.Add(msgContext.Key, msgContext);
            }
        }

        /// <summary>
        /// Get session for a client
        /// </summary>
        /// <param name="clientId">Client Id to get subscriptions</param>
        /// <returns>Subscriptions for the client</returns>
        public static MqttBrokerSession GetSession(string clientId)
        {
            if (!Sessions.ContainsKey(clientId))
            {
                return null;
            }
            else
            {
                return Sessions[clientId];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static List<MqttBrokerSession> GetSessions()
        {
            // TODO : verificare altro modo
            return new List<MqttBrokerSession>(Sessions.Values);
        }

        /// <summary>
        /// Clear session for a client (all related subscriptions)
        /// </summary>
        /// <param name="clientId">Client Id to clear session</param>
        public static void ClearSession(string clientId)
        {
            if (Sessions.ContainsKey(clientId))
            {
                // clear and remove client session
                Sessions[clientId].Clear();
                MqttBrokerSession sessionToBeRemoved;
                Sessions.TryRemove(clientId, out sessionToBeRemoved);
            }
        }
    }
}
