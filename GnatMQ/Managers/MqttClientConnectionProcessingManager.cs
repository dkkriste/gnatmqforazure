﻿namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Entities.Enums;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Handlers;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Session;
    using GnatMQForAzure.Utility;

    public class MqttClientConnectionProcessingManager : IMqttRunnable
    {
        private readonly ILogger logger;

        private readonly ConcurrentDictionary<string, MqttClientConnection> allConnectedClients;

        private readonly MqttPublishManager publishManager;

        private readonly MqttSessionManager sessionManager;

        private readonly MqttSubscriberManager subscriberManager;

        private readonly MqttUacManager uacManager;

        private readonly MqttClientConnectionReceiveManager receiveManager;

        private readonly MqttClientConnectionInflightManager inflightManager;

        private readonly MqttClientConnectionInternalEventManager internalEventManager;

        private readonly BlockingCollection<MqttRawMessage> rawMessageQueue;

        private readonly BlockingCollection<MqttClientConnection> clientConnectionsWithInflightQueuesToProcess;

        private readonly BlockingCollection<MqttClientConnection> clientConnectionsWithInternalEventQueuesToProcess;

        private bool isRunning;

        public MqttClientConnectionProcessingManager(ILogger logger)
        {
            this.logger = logger;
            rawMessageQueue = new BlockingCollection<MqttRawMessage>();
            clientConnectionsWithInflightQueuesToProcess = new BlockingCollection<MqttClientConnection>();
            clientConnectionsWithInternalEventQueuesToProcess = new BlockingCollection<MqttClientConnection>();
        }

        public void Start()
        {
            isRunning = true;
            Fx.StartThread(ProcessRawMessageQueue);
            Fx.StartThread(ProcessInflightQueue);
            Fx.StartThread(ProcessInternalEventQueue);
        }

        public void Stop()
        {
            isRunning = false;
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

        public void EnqueueClientConnectionWithInflightQueueToProcess(MqttClientConnection clientConnection)
        {
            clientConnectionsWithInflightQueuesToProcess.Add(clientConnection);
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
                    receiveManager.ProcessReceivedMessage(rawMessage);
                }
                catch (Exception exception)
                {
                    logger.LogException(exception);
                }
            }
        }

        private void ProcessInflightQueue()
        {
            while (isRunning)
            {
                try
                {
                    var clientConnection = clientConnectionsWithInflightQueuesToProcess.Take();
                    inflightManager.ProcessInflightQueue(clientConnection);
                }
                catch (Exception exception)
                {
                    logger.LogException(exception);
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
                    internalEventManager.ProcessInternalEventQueue(clientConnection);
                }
                catch (Exception exception)
                {
                    logger.LogException(exception);
                }
            }
        }

        #endregion

        void commLayer_ClientConnected(object sender, MqttClientConnectedEventArgs e)
        {
            // register event handlers from client
            e.ClientConnection.MqttMsgConnected += Client_MqttMsgConnected;
            e.ClientConnection.ConnectionClosed += Client_ConnectionClosed;

            // start client threads
            Open(e.ClientConnection);
        }

        private void Open(MqttClientConnection clientConnection)
        {
            clientConnection.isRunning = true;
            Task.Factory.StartNew(() => CheckForClientTimeout(clientConnection));
        }

        private async void CheckForClientTimeout(MqttClientConnection clientConnection)
        {
            await Task.Delay(clientConnection.Settings.TimeoutOnConnection);

            // broker need to receive the first message (CONNECT)
            // within a reasonable amount of time after TCP/IP connection
            // wait on receiving message from client with a connection timeout
            if (clientConnection.isRunning && !clientConnection.IsConnected && clientConnection.eventQueue.IsEmpty)
            {
                clientConnection.OnConnectionClosed();
            }
        }

        private void Client_MqttMsgConnected(object sender, MqttMsgConnectEventArgs e)
        {
            // [v3.1.1] session present flag
            bool sessionPresent = false;
            // [v3.1.1] generated client id for client who provides client id zero bytes length
            string clientId = null;

            MqttClientConnection clientConnection = (MqttClientConnection)sender;

            // verify message to determine CONNACK message return code to the client
            byte returnCode = this.MqttConnectVerify(e.Message);

            // [v3.1.1] if client id is zero length, the broker assigns a unique identifier to it
            clientId = (e.Message.ClientId.Length != 0) ? e.Message.ClientId : Guid.NewGuid().ToString();

            // connection "could" be accepted
            if (returnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                // check if there is a client already connected with same client Id
                MqttClientConnection clientConnectionConnected = this.GetClient(clientId);

                // force connection close to the existing client (MQTT protocol)
                if (clientConnectionConnected != null)
                {
                    this.CloseClient(clientConnectionConnected);
                }

                // add client to the collection
                this.allConnectedClients.TryAdd(clientId, clientConnection);
            }

            // connection accepted, load (if exists) client session
            if (returnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                // check if not clean session and try to recovery a session
                if (!e.Message.CleanSession)
                {
                    // create session for the client
                    MqttClientSession clientSession = new MqttClientSession(clientId);

                    // get session for the connected client
                    MqttBrokerSession session = this.sessionManager.GetSession(clientId);

                    // set inflight queue into the client session
                    if (session != null)
                    {
                        clientSession.InflightMessages = session.InflightMessages;
                        // [v3.1.1] session present flag
                        if (clientConnection.ProtocolVersion == MqttProtocolVersion.Version_3_1_1) sessionPresent = true;
                    }

                    // send CONNACK message to the client
                    clientConnection.Connack(e.Message, returnCode, clientId, sessionPresent);

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
                                this.subscriberManager.Subscribe(
                                    subscription.Topic,
                                    subscription.QosLevel,
                                    clientConnection);

                                // publish retained message on the current subscription
                                this.publishManager.PublishRetaind(subscription.Topic, clientId);
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
                    clientConnection.Connack(e.Message, returnCode, clientId, sessionPresent);

                    this.sessionManager.ClearSession(clientId);
                }
            }
            else
            {
                // send CONNACK message to the client
                clientConnection.Connack(e.Message, returnCode, clientId, sessionPresent);
            }
        }

        private void Client_ConnectionClosed(object sender, EventArgs e)
        {
            MqttClientConnection client = (MqttClientConnection)sender;

            // close the client
            CloseClient(client);
        }

        /// <summary>
        /// Close a client
        /// </summary>
        /// <param name="clientConnection">Client to close</param>
        private void CloseClient(MqttClientConnection clientConnection)
        {
            // if client is connected
            MqttClientConnection connectedClient;
            if (clientConnection.IsConnected
                && this.allConnectedClients.TryRemove(clientConnection.ClientId, out connectedClient))
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
                    List<MqttSubscription> subscriptions = this.subscriberManager.GetSubscriptionsByClient(clientConnection.ClientId);

                    if ((subscriptions != null) && (subscriptions.Count > 0))
                    {
                        this.sessionManager.SaveSession(clientConnection.ClientId, clientConnection.Session, subscriptions);

                        // TODO : persist client session if broker close
                    }
                }

                // delete client from runtime subscription
                this.subscriberManager.Unsubscribe(clientConnection);

                // close the client
                clientConnection.Close();
            }
            else
            {
                //TODO
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
            byte returnCode = MqttMsgConnack.CONN_ACCEPTED;

            // unacceptable protocol version
            if ((connect.ProtocolVersion != MqttMsgConnect.PROTOCOL_VERSION_V3_1) &&
                (connect.ProtocolVersion != MqttMsgConnect.PROTOCOL_VERSION_V3_1_1))
                returnCode = MqttMsgConnack.CONN_REFUSED_PROT_VERS;
            else
            {
                // client id length exceeded (only for old MQTT 3.1)
                if ((connect.ProtocolVersion == MqttMsgConnect.PROTOCOL_VERSION_V3_1) &&
                     (connect.ClientId.Length > MqttMsgConnect.CLIENT_ID_MAX_LENGTH))
                    returnCode = MqttMsgConnack.CONN_REFUSED_IDENT_REJECTED;
                else
                {
                    // [v.3.1.1] client id zero length is allowed but clean session must be true
                    if ((connect.ClientId.Length == 0) && (!connect.CleanSession))
                        returnCode = MqttMsgConnack.CONN_REFUSED_IDENT_REJECTED;
                    else
                    {
                        // check user authentication
                        if (!this.uacManager.UserAuthentication(connect.Username, connect.Password))
                            returnCode = MqttMsgConnack.CONN_REFUSED_USERNAME_PASSWORD;
                        // server unavailable and not authorized ?
                        else
                        {
                            // TODO : other checks on CONNECT message
                        }
                    }
                }
            }

            return returnCode;
        }

        /// <summary>
        /// Return reference to a client with a specified Id is already connected
        /// </summary>
        /// <param name="clientId">Client Id to verify</param>
        /// <returns>Reference to client</returns>
        private MqttClientConnection GetClient(string clientId)
        {
            MqttClientConnection connectedClient;
            if (allConnectedClients.TryGetValue(clientId, out connectedClient))
            {
                return connectedClient;
            }

            return null;
        }

        #endregion
    }
}