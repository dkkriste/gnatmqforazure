﻿/*
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

namespace GnatMQForAzure.Session
{
    using System.Collections.Generic;

    using GnatMQForAzure.Managers;
    using GnatMQForAzure.Messages;

    /// <summary>
    /// MQTT Broker Session
    /// </summary>
    public class MqttBrokerSession : MqttSession
    {
        /// <summary>
        /// Client related to the subscription
        /// </summary>
        public MqttClientConnection ClientConnection { get; set; }

        /// <summary>
        /// Subscriptions for the client session
        /// </summary>
        public List<MqttSubscription> Subscriptions;

        /// <summary>
        /// Outgoing messages to publish
        /// </summary>
        public Queue<MqttMsgPublish> OutgoingMessages;

        /// <summary>
        /// Constructor
        /// </summary>
        public MqttBrokerSession()
            : base()
        {
            this.ClientConnection = null;
            this.Subscriptions = new List<MqttSubscription>();
            this.OutgoingMessages = new Queue<MqttMsgPublish>();
        }

        public override void Clear()
        {
            base.Clear();
            this.ClientConnection = null;
            this.Subscriptions.Clear();
            this.OutgoingMessages.Clear();
        }
    }
}