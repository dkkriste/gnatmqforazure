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

namespace GnatMQForAzure
{
    using System.Collections.Concurrent;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Entities.Delegates;
    using GnatMQForAzure.Managers;
    using GnatMQForAzure.Utility;

    /// <summary>
    /// MQTT broker business logic
    /// </summary>
    public class MqttBroker : IPeriodicallyLoggable
    {
        // clients connected list
        private static readonly ConcurrentDictionary<string, MqttClientConnection> AllConnectedClients = new ConcurrentDictionary<string, MqttClientConnection>();

        private readonly ILogger logger;

        private readonly MqttProcessingLoadbalancer processingLoadbalancer;

        private readonly MqttClientConnectionProcessingManager[] processingManagers;

        private readonly MqttAsyncTcpSocketListener socketListener;

        // reference to User Access Control manager
        private readonly MqttUacManager uacManager;

        private MqttRawMessageManager rawMessageManager;

        private MqttAsyncTcpReceiver asyncTcpReceiver;

        private IMqttClientConnectionManager connectionManager;

        public MqttBroker(ILogger logger, MqttOptions options)
        {
            this.logger = logger;

            // create managers 
            this.uacManager = new MqttUacManager();

            MqttAsyncTcpSender.Init(options);

            this.rawMessageManager = new MqttRawMessageManager(options);
            this.asyncTcpReceiver = new MqttAsyncTcpReceiver(rawMessageManager);
            this.connectionManager = new MqttClientConnectionManager(options, asyncTcpReceiver);

            var numberOfProcessingManagersNeeded = options.MaxConnections / options.ConnectionsPrProcessingManager;
            this.processingManagers = new MqttClientConnectionProcessingManager[numberOfProcessingManagersNeeded];
            for (var i = 0; i < processingManagers.Length; i++)
            {
                processingManagers[i] = new MqttClientConnectionProcessingManager(logger, uacManager, asyncTcpReceiver);
            }

            this.processingLoadbalancer = new MqttProcessingLoadbalancer(logger, processingManagers);

            this.socketListener = new MqttAsyncTcpSocketListener(processingLoadbalancer, connectionManager, options);
        }

        /// <summary>
        /// User authentication method
        /// </summary>
        public MqttUserAuthenticationDelegate UserAuth
        {
            get { return this.uacManager.UserAuth; }
            set { this.uacManager.UserAuth = value; }
        }

        public MqttSubscribeAuthenticationDelegate SubscribeAuthentication
        {
            get { return uacManager.SubscribeAuthentication; }
            set { uacManager.SubscribeAuthentication = value; }
        }

        public MqttPublishAuthenticationDelegate PublishAuthentication
        {
            get { return uacManager.PublishAuthentication; }
            set { uacManager.PublishAuthentication = value; }
        }

        /// <summary>
        /// Return reference to a client with a specified Id is already connected
        /// </summary>
        /// <param name="clientId">Client Id to verify</param>
        /// <returns>Reference to client</returns>
        public static MqttClientConnection GetClientConnection(string clientId)
        {
            MqttClientConnection connectedClient;
            if (AllConnectedClients.TryGetValue(clientId, out connectedClient))
            {
                return connectedClient;
            }

            return null;
        }

        public static bool TryAddClientConnection(string clientId, MqttClientConnection clientConnection)
        {
            return AllConnectedClients.TryAdd(clientId, clientConnection);
        }

        public static bool TryRemoveClientConnection(string clientId, out MqttClientConnection clientConnection)
        {
            return AllConnectedClients.TryRemove(clientId, out clientConnection);
        }

        /// <summary>
        /// Start broker
        /// </summary>
        public void Start()
        {
            socketListener.Start();
            processingLoadbalancer.Start();
            RetainedMessageManager.Start();

            foreach (var processingManager in processingManagers)
            {
                processingManager.Start();
            }
        }

        /// <summary>
        /// Stop broker
        /// </summary>
        public void Stop()
        {
            socketListener.Stop();
            processingLoadbalancer.Stop();
            RetainedMessageManager.Stop();
            
            foreach (var processingManager in processingManagers)
            {
                processingManager.Stop();
            }
        }

        public void PeriodicLogging()
        {
            logger.LogMetric(this, LoggerConstants.NumberOfConnectedClients, AllConnectedClients.Count);

            processingLoadbalancer.PeriodicLogging();

            foreach (var processingManager in processingManagers)
            {
                processingManager.PeriodicLogging();
            }
        }
    }
}
