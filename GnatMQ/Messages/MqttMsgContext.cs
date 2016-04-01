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

namespace GnatMQForAzure.Messages
{
    /// <summary>
    /// Context for MQTT message
    /// </summary>
    public class MqttMsgContext
    {
        /// <summary>
        /// MQTT message
        /// </summary>
        public MqttMsgBase Message { get; set; }

        /// <summary>
        /// MQTT message state
        /// </summary>
        public MqttMsgState State { get; set; }

        /// <summary>
        /// Flow of the message
        /// </summary>
        public MqttMsgFlow Flow { get; set; }

        /// <summary>
        /// Timestamp in ticks (for retry)
        /// </summary>
        public int Timestamp { get; set; }

        /// <summary>
        /// Attempt (for retry)
        /// </summary>
        public int Attempt { get; set; }

        /// <summary>
        /// Unique key
        /// </summary>
        public string Key => this.Flow + "_" + this.Message.MessageId;
    }
}
