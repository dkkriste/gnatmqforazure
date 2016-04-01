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

namespace GnatMQForAzure.Session
{
    using System.Collections;
    using System.Collections.Concurrent;

    using GnatMQForAzure.Messages;

    /// <summary>
    /// MQTT Session base class
    /// </summary>
    public abstract class MqttSession
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected MqttSession()
            : this(null)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="clientId">Client Id to create session</param>
        protected MqttSession(string clientId)
        {
            this.ClientId = clientId;
            this.InflightMessages = new ConcurrentDictionary<string, MqttMsgContext>();
        }

        /// <summary>
        /// Client Id
        /// </summary>
        public string ClientId { get; set; }

        /// <summary>
        /// Messages inflight during session
        /// </summary>
        public ConcurrentDictionary<string, MqttMsgContext> InflightMessages { get; set; }

        /// <summary>
        /// Clean session
        /// </summary>
        public virtual void Clear()
        {
            this.ClientId = null;
            this.InflightMessages.Clear();
        }
    }
}
