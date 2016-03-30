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
    using GnatMQForAzure.Entities.Delegates;

    /// <summary>
    /// Manager for User Access Control
    /// </summary>
    public class MqttUacManager
    {
        /// <summary>
        /// User authentication method
        /// </summary>
        public MqttUserAuthenticationDelegate UserAuth { get; set; }

        public MqttSubscribeAuthenticationDelegate SubscribeAuthentication { get; set; }

        public MqttPublishAuthenticationDelegate PublishAuthentication { get; set; }

        /// <summary>
        /// Execute user authentication
        /// </summary>
        /// <param name="username">Username</param>
        /// <param name="password">Password</param>
        /// <returns>Access granted or not</returns>
        public bool AuthenticateUser(string username, string password)
        {
            if (this.UserAuth == null)
            {
                return true;
            }
            else
            {
                return this.UserAuth(username, password);
            }
        }

        public bool AuthenticateSubscriber(MqttClientConnection clientConnection, string topic)
        {
            if (SubscribeAuthentication == null)
            {
                return true;
            }
            else
            {
                return SubscribeAuthentication(clientConnection, topic);
            }
        }

        public bool AuthenticatePublish(MqttClientConnection clientConnection, string topic)
        {
            if (PublishAuthentication == null)
            {
                return true;
            }
            else
            {
                return PublishAuthentication(clientConnection, topic);
            }
        }
    }
}
