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
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Exceptions;

    /// <summary>
    /// Class for DISCONNECT message from client to broker
    /// </summary>
    public class MqttMsgDisconnect : MqttMsgBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public MqttMsgDisconnect()
        {
            this.type = MQTT_MSG_DISCONNECT_TYPE;
        }

        public static MqttMsgDisconnect Parse(byte fixedHeaderFirstByte, byte protocolVersion)
        {
            MqttMsgDisconnect msg = new MqttMsgDisconnect();

            if (protocolVersion == MqttMsgConnect.PROTOCOL_VERSION_V3_1_1)
            {
                // [v3.1.1] check flag bits
                if ((fixedHeaderFirstByte & MSG_FLAG_BITS_MASK) != MQTT_MSG_DISCONNECT_FLAG_BITS)
                {
                    throw new MqttClientException(MqttClientErrorCode.InvalidFlagBits);
                }
            }

            return msg;
        }

        public override byte[] GetBytes(byte protocolVersion)
        {
            byte[] buffer = new byte[2];
            int index = 0;

            // first fixed header byte
            if (protocolVersion == MqttMsgConnect.PROTOCOL_VERSION_V3_1_1)
                buffer[index++] = (MQTT_MSG_DISCONNECT_TYPE << MSG_TYPE_OFFSET) | MQTT_MSG_DISCONNECT_FLAG_BITS; // [v.3.1.1]
            else
                buffer[index++] = (MQTT_MSG_DISCONNECT_TYPE << MSG_TYPE_OFFSET);
            buffer[index++] = 0x00;

            return buffer;
        }
    }
}
