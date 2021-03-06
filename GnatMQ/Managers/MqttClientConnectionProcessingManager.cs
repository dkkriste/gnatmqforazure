﻿namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Entities.Enums;
    using GnatMQForAzure.Events;
    using GnatMQForAzure.Handlers;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Session;
    using GnatMQForAzure.Utility;

    public class MqttClientConnectionProcessingManager : IMqttRunnable, IMqttClientConnectionStarter, IPeriodicallyLoggable
    {
        private readonly ILogger logger;

        private readonly IMqttClientConnectionManager clientConnectionManager;

        private readonly MqttPublishManager publishManager;

        private readonly MqttUacManager uacManager;

        private readonly MqttAsyncTcpReceiver asyncTcpReceiver;

        private readonly BlockingCollection<MqttRawMessage> rawMessageQueue;

        private readonly BlockingCollection<InflightQueueProcessEvent> clientConnectionsWithInflightQueuesToProcess;

        private readonly BlockingCollection<MqttClientConnection> clientConnectionsWithInternalEventQueuesToProcess;

        private bool isRunning;

        private int numberOfConnectedClients;

        private int numberOfAssignedClients;

        #region Logging

        private int numberOfRawMessagsProcessed;

        private int numberOfInflightQueuesProcessed;

        private int numberOfInternalEventQueuesProcessed;

        #endregion

        public MqttClientConnectionProcessingManager(ILogger logger, IMqttClientConnectionManager clientConnectionManager, MqttUacManager uacManager, MqttAsyncTcpReceiver asyncTcpReceiver)
        {
            this.logger = logger;
            this.clientConnectionManager = clientConnectionManager;
            this.uacManager = uacManager;
            this.asyncTcpReceiver = asyncTcpReceiver;
            publishManager = new MqttPublishManager(logger);
            rawMessageQueue = new BlockingCollection<MqttRawMessage>();
            clientConnectionsWithInflightQueuesToProcess = new BlockingCollection<InflightQueueProcessEvent>();
            clientConnectionsWithInternalEventQueuesToProcess = new BlockingCollection<MqttClientConnection>();
        }

        public int ConnectedClients => numberOfConnectedClients;

        public int AssignedClients => numberOfAssignedClients;

        public void Start()
        {
            isRunning = true;
            publishManager.Start();
            Fx.StartThread(ProcessRawMessageQueue);
            Fx.StartThread(ProcessInflightQueue);
            Fx.StartThread(ProcessInternalEventQueue);
        }

        public void Stop()
        {
            isRunning = false;
            publishManager.Stop();
        }

        public void PeriodicLogging()
        {
            logger.LogMetric(this, LoggerConstants.RawMessageQueueSize, rawMessageQueue.Count);
            logger.LogMetric(this, LoggerConstants.InflightQueuesToProcessSize, clientConnectionsWithInflightQueuesToProcess.Count);
            logger.LogMetric(this, LoggerConstants.EventQueuesToProcessSize, clientConnectionsWithInternalEventQueuesToProcess.Count);

            var rawMessageCopy = numberOfRawMessagsProcessed;
            var inflightCopy = numberOfInflightQueuesProcessed;
            var internalEventCopy = numberOfInternalEventQueuesProcessed;
            logger.LogMetric(this, LoggerConstants.NumberOfRawMessagsProcessed, rawMessageCopy);
            logger.LogMetric(this, LoggerConstants.NumberOfInflightQueuesProcessed, inflightCopy);
            logger.LogMetric(this, LoggerConstants.NumberOfInternalEventQueuesProcessed, internalEventCopy);

            Interlocked.Add(ref numberOfRawMessagsProcessed, -rawMessageCopy);
            Interlocked.Add(ref numberOfInflightQueuesProcessed, -inflightCopy);
            Interlocked.Add(ref numberOfInternalEventQueuesProcessed, -internalEventCopy);

            publishManager.PeriodicLogging();
        }

        #region Enqueuing

        public void EnqueueRawMessageForProcessing(MqttRawMessage rawMessage)
        {
            rawMessageQueue.Add(rawMessage);
        }

        public void EnqueueClientConnectionWithInternalEventQueueToProcess(MqttClientConnection clientConnection)
        {
            clientConnectionsWithInternalEventQueuesToProcess.Add(clientConnection);
        }

        public void EnqueueClientConnectionWithInflightQueueToProcess(InflightQueueProcessEvent processEvent)
        {
            clientConnectionsWithInflightQueuesToProcess.Add(processEvent);
        }

        #endregion

        #region ProcessingThreads

        private void ProcessRawMessageQueue()
        {
            while (isRunning)
            {
                try
                {
                    var rawMessage = rawMessageQueue.Take();
                    MqttMessageToClientConnectionManager.ProcessReceivedMessage(rawMessage);
                    Interlocked.Increment(ref numberOfRawMessagsProcessed);
                }
                catch (Exception exception)
                {
                    logger.LogException(this, exception);
                }
            }
        }

        private void ProcessInflightQueue()
        {
            while (isRunning)
            {
                try
                {
                    var processEvent = clientConnectionsWithInflightQueuesToProcess.Take();
                    MqttClientConnectionInflightManager.ProcessInflightQueue(processEvent);
                    Interlocked.Increment(ref numberOfInflightQueuesProcessed);
                }
                catch (Exception exception)
                {
                    logger.LogException(this, exception);
                }
            }
        }

        private void ProcessInternalEventQueue()
        {
            while (isRunning)
            {
                try
                {
                    var clientConnection = clientConnectionsWithInternalEventQueuesToProcess.Take();
                    MqttClientConnectionInternalEventManager.ProcessInternalEventQueue(clientConnection);
                    Interlocked.Increment(ref numberOfInternalEventQueuesProcessed);
                }
                catch (Exception exception)
                {
                    logger.LogException(this, exception);
                }
            }
        }

        #endregion

        public void OpenClientConnection(MqttClientConnection clientConnection)
        {
            numberOfAssignedClients++;
            clientConnection.IsRunning = true;
            clientConnection.ProcessingManager = this;
            asyncTcpReceiver.StartReceive(clientConnection.ReceiveSocketAsyncEventArgs);
            Task.Factory.StartNew(() => CheckForClientTimeout(clientConnection));
        }

        private void CloseClientConnection(MqttClientConnection clientConnection)
        {
            // stop receiving thread
            clientConnection.IsRunning = false;

            clientConnection.IsConnected = false;
        }

        private async void CheckForClientTimeout(MqttClientConnection clientConnection)
        {
            await Task.Delay(clientConnection.Settings.TimeoutOnConnection);

            // broker need to receive the first message (CONNECT)
            // within a reasonable amount of time after TCP/IP connection
            // wait on receiving message from client with a connection timeout
            if (clientConnection.IsRunning && !clientConnection.IsConnected && clientConnection.EventQueue.IsEmpty)
            {
                clientConnection.OnConnectionClosed();
            }
        }

        public void OnMqttMsgConnected(MqttClientConnection clientConnection, MqttMsgConnect message)
        {
            clientConnection.ProtocolVersion = (MqttProtocolVersion)message.ProtocolVersion;

            // verify message to determine CONNACK message return code to the client
            byte returnCode = MqttConnectVerify(message);

            // [v3.1.1] if client id is zero length, the broker assigns a unique identifier to it
            var clientId = (message.ClientId.Length != 0) ? message.ClientId : Guid.NewGuid().ToString();

            // connection "could" be accepted
            if (returnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                // check if there is a client already connected with same client Id
                MqttClientConnection clientConnectionConnected = MqttBroker.GetClientConnection(clientId);

                // force connection close to the existing client (MQTT protocol)
                if (clientConnectionConnected != null)
                {
                    OnConnectionClosed(clientConnectionConnected);
                }

                // add client to the collection
                MqttBroker.TryAddClientConnection(clientId, clientConnection);
                Interlocked.Increment(ref numberOfConnectedClients);
            }

            // connection accepted, load (if exists) client session
            if (returnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                // check if not clean session and try to recovery a session
                if (!message.CleanSession)
                {
                    // create session for the client
                    MqttClientSession clientSession = new MqttClientSession(clientId);

                    // get session for the connected client
                    MqttBrokerSession session = MqttSessionManager.GetSession(clientId);

                    // [v3.1.1] session present flag
                    bool sessionPresent = false;

                    // set inflight queue into the client session
                    if (session != null)
                    {
                        clientSession.InflightMessages = session.InflightMessages;
                        // [v3.1.1] session present flag
                        if (clientConnection.ProtocolVersion == MqttProtocolVersion.Version_3_1_1) sessionPresent = true;
                    }

                    // send CONNACK message to the client
                    MqttOutgoingMessageManager.Connack(clientConnection, message, returnCode, clientId, sessionPresent);

                    // load/inject session to the client
                    clientConnection.LoadSession(clientSession);

                    if (session != null)
                    {
                        // set reference to connected client into the session
                        session.ClientConnection = clientConnection;

                        // there are saved subscriptions
                        if (session.Subscriptions != null)
                        {
                            // register all subscriptions for the connected client
                            foreach (MqttSubscription subscription in session.Subscriptions)
                            {
                                MqttSubscriberManager.Subscribe(
                                    subscription.Topic,
                                    subscription.QosLevel,
                                    clientConnection);

                                // publish retained message on the current subscription
                                RetainedMessageManager.PublishRetaind(subscription.Topic, clientConnection);
                            }
                        }

                        // there are saved outgoing messages
                        if (session.OutgoingMessages.Count > 0)
                        {
                            // publish outgoing messages for the session
                            this.publishManager.PublishSession(session.ClientId);
                        }
                    }
                }
                // requested clean session
                else
                {
                    // send CONNACK message to the client
                    MqttOutgoingMessageManager.Connack(clientConnection, message, returnCode, clientId, false);

                    MqttSessionManager.ClearSession(clientId);
                }
            }
            else
            {
                // send CONNACK message to the client
                MqttOutgoingMessageManager.Connack(clientConnection, message, returnCode, clientId, false);
            }
        }

        public void OnConnectionClosed(MqttClientConnection clientConnection)
        {
            // if client is connected
            MqttClientConnection connectedClient;
            if (clientConnection.IsConnected && MqttBroker.TryRemoveClientConnection(clientConnection.ClientId, out connectedClient))
            {
                // client has a will message
                if (clientConnection.WillFlag)
                {
                    // create the will PUBLISH message
                    MqttMsgPublish publish = new MqttMsgPublish(clientConnection.WillTopic, Encoding.UTF8.GetBytes(clientConnection.WillMessage), false, clientConnection.WillQosLevel, false);

                    // publish message through publisher manager
                    this.publishManager.Publish(publish);
                }

                // if not clean session
                if (!clientConnection.CleanSession)
                {
                    List<MqttSubscription> subscriptions = clientConnection.Subscriptions.Values.ToList();

                    if (subscriptions.Count > 0)
                    {
                        MqttSessionManager.SaveSession(clientConnection.ClientId, clientConnection.Session, subscriptions);

                        // TODO : persist client session if broker close
                    }
                }

                foreach (var topic in clientConnection.Subscriptions.Keys)
                {
                    // delete client from runtime subscription
                    MqttSubscriberManager.Unsubscribe(topic, clientConnection);
                }

                // close the client
                CloseClientConnection(clientConnection);
                clientConnectionManager.ReturnConnection(clientConnection);
                Interlocked.Decrement(ref numberOfConnectedClients);
            }
        }

        public void OnMqttMsgPublishReceived(MqttClientConnection clientConnection, MqttMsgPublish msg)
        {
            if (uacManager.AuthenticatePublish(clientConnection, msg.Topic))
            {
                // create PUBLISH message to publish
                // [v3.1.1] DUP flag from an incoming PUBLISH message is not propagated to subscribers
                //          It should be set in the outgoing PUBLISH message based on transmission for each subscriber
                MqttMsgPublish publish = new MqttMsgPublish(msg.Topic, msg.Message, false, msg.QosLevel, msg.Retain);

                // publish message through publisher manager
                this.publishManager.Publish(publish);
            }
        }

        public void OnMqttMsgSubscribeReceived(MqttClientConnection clientConnection, ushort messageId, string[] topics, byte[] qosLevels)
        {
            var wasSubscribed = false;
            for (var i = 0; i < topics.Length; i++)
            {
                if (uacManager.AuthenticateSubscriber(clientConnection, topics[i]))
                {
                    // TODO : business logic to grant QoS levels based on some conditions ?
                    // now the broker granted the QoS levels requested by client

                    // subscribe client for each topic and QoS level requested
                    MqttSubscriberManager.Subscribe(topics[i], qosLevels[i], clientConnection);
                    wasSubscribed = true;
                }
            }

            if (wasSubscribed)
            {
                // send SUBACK message to the client
                MqttOutgoingMessageManager.Suback(clientConnection, messageId, qosLevels);

                foreach (var topic in topics)
                {
                    // publish retained message on the current subscription
                    RetainedMessageManager.PublishRetaind(topic, clientConnection);
                }
            }
        }

        #region Helpers

        /// <summary>
        /// Check CONNECT message to accept or not the connection request 
        /// </summary>
        /// <param name="connect">CONNECT message received from client</param>
        /// <returns>Return code for CONNACK message</returns>
        private byte MqttConnectVerify(MqttMsgConnect connect)
        {
            // unacceptable protocol version
            if ((connect.ProtocolVersion != MqttMsgConnect.PROTOCOL_VERSION_V3_1)
                && (connect.ProtocolVersion != MqttMsgConnect.PROTOCOL_VERSION_V3_1_1))
            {
                return MqttMsgConnack.CONN_REFUSED_PROT_VERS;
            }

            // client id length exceeded (only for old MQTT 3.1)
            if ((connect.ProtocolVersion == MqttMsgConnect.PROTOCOL_VERSION_V3_1)
                && (connect.ClientId.Length > MqttMsgConnect.CLIENT_ID_MAX_LENGTH))
            {
                return MqttMsgConnack.CONN_REFUSED_IDENT_REJECTED;
            }

            // [v.3.1.1] client id zero length is allowed but clean session must be true
            if ((connect.ClientId.Length == 0) && (!connect.CleanSession))
            {
                return MqttMsgConnack.CONN_REFUSED_IDENT_REJECTED;
            }

            // check user authentication
            if (!this.uacManager.AuthenticateUser(connect.Username, connect.Password))
            {
                return MqttMsgConnack.CONN_REFUSED_USERNAME_PASSWORD;
            }
            // server unavailable and not authorized ?
            else
            {
                // TODO : other checks on CONNECT message
            }

            return MqttMsgConnack.CONN_ACCEPTED;
        }

        #endregion
    }
}