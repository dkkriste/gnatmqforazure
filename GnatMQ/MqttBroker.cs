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
    using System.Net.Security;
    using System.Security.Cryptography.X509Certificates;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Entities.Delegates;
    using GnatMQForAzure.Entities.Enums;
    using GnatMQForAzure.Handlers;
    using GnatMQForAzure.Managers;

    /// <summary>
    /// MQTT broker business logic
    /// </summary>
    public class MqttBroker
    {
        // clients connected list
        private readonly ConcurrentDictionary<string, MqttClientConnection> allConnectedClients;

        private readonly MqttProcessingLoadbalancer processingLoadbalancer;

        private readonly MqttClientConnectionProcessingManager[] processingManagers;

        private readonly MqttAsyncTcpSocketListener socketListener;

        // reference to User Access Control manager
        private readonly MqttUacManager uacManager;

        private MqttRawMessageManager rawMessageManager;

        private MqttAsyncTcpReceiver asyncTcpReceiver;

        private IMqttClientConnectionManager connectionManager;

        // MQTT broker settings
        private MqttSettings settings;

        /// <summary>
        /// Constructor (TCP/IP communication layer on port 1883 and default settings)
        /// </summary>
        public MqttBroker()
            : this(new MqttTcpCommunicationLayer(MqttSettings.MQTT_BROKER_DEFAULT_PORT), MqttSettings.Instance)
        {
        }

        /// <summary>
        /// Constructor (TCP/IP communication layer on port 8883 with SSL/TLS and default settings)
        /// </summary>
        /// <param name="serverCert">X509 Server certificate</param>
        /// <param name="sslProtocol">SSL/TLS protocol versiokn</param>
        public MqttBroker(X509Certificate serverCert, MqttSslProtocols sslProtocol)
            : this(new MqttTcpCommunicationLayer(MqttSettings.MQTT_BROKER_DEFAULT_SSL_PORT, true, serverCert, sslProtocol, null, null), MqttSettings.Instance)
        {
        }

        /// <summary>
        /// Constructor (TCP/IP communication layer on port 8883 with SSL/TLS and default settings)
        /// </summary>
        /// <param name="serverCert">X509 Server certificate</param>
        /// <param name="sslProtocol">SSL/TLS protocol version</param>
        /// <param name="userCertificateSelectionCallback">A RemoteCertificateValidationCallback delegate responsible for validating the certificate supplied by the remote party</param>
        /// <param name="userCertificateValidationCallback">A LocalCertificateSelectionCallback delegate responsible for selecting the certificate used for authentication</param>
        public MqttBroker(
            X509Certificate serverCert, 
            MqttSslProtocols sslProtocol,
            RemoteCertificateValidationCallback userCertificateValidationCallback,
            LocalCertificateSelectionCallback userCertificateSelectionCallback)
            : this(new MqttTcpCommunicationLayer(MqttSettings.MQTT_BROKER_DEFAULT_SSL_PORT, true, serverCert, sslProtocol, userCertificateValidationCallback, userCertificateSelectionCallback), MqttSettings.Instance)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="commLayer">Communication layer to use (TCP)</param>
        /// <param name="settings">Broker settings</param>
        public MqttBroker(IMqttCommunicationLayer commLayer, MqttSettings settings)
        {
            var options = new MqttOptions();
            ILogger logger = null;
            // MQTT broker settings
            this.settings = settings;

            // create managers 
            this.uacManager = new MqttUacManager();

            this.allConnectedClients = new ConcurrentDictionary<string, MqttClientConnection>();

            var numberOfProcessingManagersNeeded = options.MaxConnections / options.ConnectionsPrProcessingManager;
            this.processingManagers = new MqttClientConnectionProcessingManager[numberOfProcessingManagersNeeded];
            for (var i = 0; i < processingManagers.Length; i++)
            {
                processingManagers[i] = new MqttClientConnectionProcessingManager(logger, allConnectedClients, uacManager);
            }

            this.processingLoadbalancer = new MqttProcessingLoadbalancer(logger, processingManagers);

            this.rawMessageManager = new MqttRawMessageManager(options);
            this.asyncTcpReceiver = new MqttAsyncTcpReceiver(rawMessageManager);
            this.connectionManager = new MqttClientConnectionManager(options, asyncTcpReceiver);
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

        /// <summary>
        /// Start broker
        /// </summary>
        public void Start()
        {
            socketListener.Start();
            processingLoadbalancer.Start();

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
            
            foreach (var processingManager in processingManagers)
            {
                processingManager.Stop();
            }

            // TODO close connection with all clients
            //foreach (MqttClientConnection client in this.allConnectedClients)
            //{
            //    client.Close();
            //}
        }
    }
}
