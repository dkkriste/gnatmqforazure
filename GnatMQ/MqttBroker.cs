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
    using System.Collections.Generic;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Entities.Delegates;
    using GnatMQForAzure.Managers;
    using GnatMQForAzure.Utility;

    public class MqttBroker : IPeriodicallyLoggable
    {
        // clients connected list
        private static readonly ConcurrentDictionary<string, MqttClientConnection> AllConnectedClients = new ConcurrentDictionary<string, MqttClientConnection>();

        private readonly ILogger logger;

        private readonly MqttProcessingLoadbalancer processingLoadbalancer;

        private readonly MqttClientConnectionProcessingManager[] processingManagers;

        private readonly MqttAsyncTcpSocketListener socketListener;

        private readonly MqttUacManager uacManager;

        private readonly MqttRawMessageManager rawMessageManager;

        private readonly MqttAsyncTcpReceiver asyncTcpReceiver;

        private readonly IMqttClientConnectionManager connectionManager;

        public MqttBroker(ILogger logger, MqttOptions options)
        {
            MqttAsyncTcpSender.Init(options);

            this.logger = logger;
            this.uacManager = new MqttUacManager();
            this.rawMessageManager = new MqttRawMessageManager(options);
            this.asyncTcpReceiver = new MqttAsyncTcpReceiver(rawMessageManager);
            this.connectionManager = new MqttClientConnectionManager(logger, options, asyncTcpReceiver);

            var numberOfProcessingManagersNeeded = options.MaxConnections / options.ConnectionsPrProcessingManager;
            this.processingManagers = new MqttClientConnectionProcessingManager[numberOfProcessingManagersNeeded];
            for (var i = 0; i < processingManagers.Length; i++)
            {
                processingManagers[i] = new MqttClientConnectionProcessingManager(logger, connectionManager, uacManager, asyncTcpReceiver);
            }

            this.processingLoadbalancer = new MqttProcessingLoadbalancer(logger, processingManagers);

            this.socketListener = new MqttAsyncTcpSocketListener(processingLoadbalancer, connectionManager, options);
        }

        public MqttUserAuthenticationDelegate UserAuth
        {
            get { return this.uacManager.UserAuth; }
            set { this.uacManager.UserAuth = value; }
        }

        public MqttSubscribeAuthenticationDelegate SubscribeAuth
        {
            get { return uacManager.SubscribeAuthentication; }
            set { uacManager.SubscribeAuthentication = value; }
        }

        public MqttPublishAuthenticationDelegate PublishAuth
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

        public static ICollection<MqttClientConnection> GetAllConnectedClients()
        {
            return AllConnectedClients.Values;
        }

        public static bool TryAddClientConnection(string clientId, MqttClientConnection clientConnection)
        {
            return AllConnectedClients.TryAdd(clientId, clientConnection);
        }

        public static bool TryRemoveClientConnection(string clientId, out MqttClientConnection clientConnection)
        {
            return AllConnectedClients.TryRemove(clientId, out clientConnection);
        }

        public void Start()
        {
            socketListener.Start();
            processingLoadbalancer.Start();
            RetainedMessageManager.Start();
            MqttKeepAliveManager.Start();

            foreach (var processingManager in processingManagers)
            {
                processingManager.Start();
            }
        }

        public void Stop()
        {
            socketListener.Stop();
            processingLoadbalancer.Stop();
            RetainedMessageManager.Stop();
            MqttKeepAliveManager.Stop();

            foreach (var processingManager in processingManagers)
            {
                processingManager.Stop();
            }
        }

        public void PeriodicLogging()
        {
            logger.LogMetric(this, LoggerConstants.NumberOfConnectedClients, AllConnectedClients.Count);

            processingLoadbalancer.PeriodicLogging();
            connectionManager.PeriodicLogging();

            foreach (var processingManager in processingManagers)
            {
                processingManager.PeriodicLogging();
            }
        }
    }
}
