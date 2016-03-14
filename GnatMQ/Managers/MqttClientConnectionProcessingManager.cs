namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;

    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Entities.Enums;
    using GnatMQForAzure.Handlers;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Session;
    using GnatMQForAzure.Utility;

    public class MqttClientConnectionProcessingManager : IMqttRunnable, IMqttClientConnectionStarter
    {
        private readonly ILogger logger;

        private readonly ConcurrentDictionary<string, MqttClientConnection> allConnectedClients;

        private readonly MqttPublishManager publishManager;

        private readonly MqttUacManager uacManager;

        private readonly BlockingCollection<MqttRawMessage> rawMessageQueue;

        private readonly BlockingCollection<MqttClientConnection> clientConnectionsWithInflightQueuesToProcess;

        private readonly BlockingCollection<MqttClientConnection> clientConnectionsWithInternalEventQueuesToProcess;

        private bool isRunning;

        private int numberOfConnectedClients;

        private int numberOfAssignedClients;

        public MqttClientConnectionProcessingManager(ILogger logger, ConcurrentDictionary<string, MqttClientConnection> allConnectedClients, MqttUacManager uacManager)
        {
            this.logger = logger;
            this.allConnectedClients = allConnectedClients;
            this.uacManager = uacManager;
            rawMessageQueue = new BlockingCollection<MqttRawMessage>();
            clientConnectionsWithInflightQueuesToProcess = new BlockingCollection<MqttClientConnection>();
            clientConnectionsWithInternalEventQueuesToProcess = new BlockingCollection<MqttClientConnection>();
        }

        public int ConnectedClients => numberOfConnectedClients;

        public int AssignedClients => numberOfAssignedClients;

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
                    MqttMessageToClientConnectionManager.ProcessReceivedMessage(rawMessage);
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
                    MqttClientConnectionInflightManager.ProcessInflightQueue(clientConnection);
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
                    MqttClientConnectionInternalEventManager.ProcessInternalEventQueue(clientConnection);
                }
                catch (Exception exception)
                {
                    logger.LogException(exception);
                }
            }
        }

        #endregion

        public void OpenClientConnection(MqttClientConnection clientConnection)
        {
            numberOfAssignedClients++;
            clientConnection.IsRunning = true;
            clientConnection.ProcessingManager = this;
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
                MqttClientConnection clientConnectionConnected = GetClient(clientId);

                // force connection close to the existing client (MQTT protocol)
                if (clientConnectionConnected != null)
                {
                    OnConnectionClosed(clientConnectionConnected);
                }

                // add client to the collection
                allConnectedClients.TryAdd(clientId, clientConnection);
                numberOfConnectedClients++;
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
                    List<MqttSubscription> subscriptions = MqttSubscriberManager.GetSubscriptionsByClient(clientConnection.ClientId);

                    if ((subscriptions != null) && (subscriptions.Count > 0))
                    {
                        MqttSessionManager.SaveSession(clientConnection.ClientId, clientConnection.Session, subscriptions);

                        // TODO : persist client session if broker close
                    }
                }

                // delete client from runtime subscription
                MqttSubscriberManager.Unsubscribe(clientConnection);

                // close the client
                CloseClientConnection(clientConnection);
                numberOfConnectedClients--;
            }
            else
            {
                //TODO
                numberOfAssignedClients--;
            }
        }

        public void OnMqttMsgPublishReceived(MqttClientConnection clientConnection, MqttMsgPublish msg)
        {
            // create PUBLISH message to publish
            // [v3.1.1] DUP flag from an incoming PUBLISH message is not propagated to subscribers
            //          It should be set in the outgoing PUBLISH message based on transmission for each subscriber
            MqttMsgPublish publish = new MqttMsgPublish(msg.Topic, msg.Message, false, msg.QosLevel, msg.Retain);

            // publish message through publisher manager
            this.publishManager.Publish(publish);
        }

        public void OnMqttMsgSubscribeReceived(MqttClientConnection clientConnection, ushort messageId, string[] topics, byte[] qosLevels)
        {
            for (int i = 0; i < topics.Length; i++)
            {
                // TODO : business logic to grant QoS levels based on some conditions ?
                //        now the broker granted the QoS levels requested by client

                // subscribe client for each topic and QoS level requested
                MqttSubscriberManager.Subscribe(topics[i], qosLevels[i], clientConnection);
            }

            // send SUBACK message to the client
            MqttOutgoingMessageManager.Suback(clientConnection, messageId, qosLevels);

            for (int i = 0; i < topics.Length; i++)
            {
                // publish retained message on the current subscription
                this.publishManager.PublishRetaind(topics[i], clientConnection.ClientId);
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
            if (!this.uacManager.UserAuthentication(connect.Username, connect.Password))
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