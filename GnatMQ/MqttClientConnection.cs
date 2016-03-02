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
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Net;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Entities.Enums;
    using GnatMQForAzure.Events;
    using GnatMQForAzure.Exceptions;
    using GnatMQForAzure.Managers;
    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Session;
    using GnatMQForAzure.Utility;

    /// <summary>
    /// MQTT Client Connection
    /// </summary>
    public class MqttClientConnection
    {
        #region Fields

        // running status of threads
        public bool isRunning;

        // event for signaling synchronous receive
        public AutoResetEvent syncEndReceiving;
        
        // message received
        public MqttMsgBase msgReceived;

        // exeption thrown during receiving
        public Exception exReceiving;

        // keep alive period (in ms)
        public int keepAlivePeriod;
        
        // events for signaling on keep alive thread
        public AutoResetEvent keepAliveEvent;

        public AutoResetEvent keepAliveEventEnd;
        
        // last communication time in ticks
        public int lastCommTime;

        // channel to communicate over the network
        public IMqttNetworkChannel channel;

        // inflight messages queue
        public ConcurrentQueue<MqttMsgContext> inflightQueue;
        
        // internal queue for received messages about inflight messages
        public ConcurrentQueue<MqttMsgBase> internalQueue;
        
        // internal queue for dispatching events
        public ConcurrentQueue<InternalEvent> eventQueue;
        // session

        // current message identifier generated
        private ushort messageIdCounter = 0;

        // connection is closing due to peer
        public bool isConnectionClosing;

        public readonly SocketAsyncEventArgs ReceiveSocketAsyncEventArgs;

        public readonly int ReceiveSocketOffset;

        public readonly int ReceiveSocketBufferSize;

        public int PreviouslyRead;

        public MqttClientConnectionProcessingManager ProcessingManager;

        #endregion

        #region Constructors

        public MqttClientConnection(SocketAsyncEventArgs receiveSocketAsyncEventArgs)
        {
            ReceiveSocketAsyncEventArgs = receiveSocketAsyncEventArgs;
            ReceiveSocketOffset = receiveSocketAsyncEventArgs.Offset;
            ReceiveSocketBufferSize = receiveSocketAsyncEventArgs.Count;
            ReceiveSocketAsyncEventArgs.UserToken = this;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="channel">Network channel for communication</param>
        public MqttClientConnection(IMqttNetworkChannel channel)
        {
            // set default MQTT protocol version (default is 3.1.1)
            this.ProtocolVersion = MqttProtocolVersion.Version_3_1_1;

            this.channel = channel;

            // reference to MQTT settings
            this.Settings = MqttSettings.Instance;

            // client not connected yet (CONNACK not send from client), some default values
            this.IsConnected = false;
            this.ClientId = null;
            this.CleanSession = true;

            this.keepAliveEvent = new AutoResetEvent(false);

            // queue for handling inflight messages (publishing and acknowledge)
            this.inflightQueue = new ConcurrentQueue<MqttMsgContext>();

            // queue for received message
            this.eventQueue = new ConcurrentQueue<InternalEvent>();
            this.internalQueue = new ConcurrentQueue<MqttMsgBase>();

            // session
            this.Session = null;
        }

        #endregion
    
        #region Delegates and events

        /// <summary>
        /// Delagate that defines event handler for PUBLISH message received
        /// </summary>
        public delegate void MqttMsgPublishEventHandler(object sender, MqttMsgPublishEventArgs e);

        /// <summary>
        /// Delegate that defines event handler for published message
        /// </summary>
        public delegate void MqttMsgPublishedEventHandler(object sender, MqttMsgPublishedEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for subscribed topic
        /// </summary>
        public delegate void MqttMsgSubscribedEventHandler(object sender, MqttMsgSubscribedEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for unsubscribed topic
        /// </summary>
        public delegate void MqttMsgUnsubscribedEventHandler(object sender, MqttMsgUnsubscribedEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for SUBSCRIBE message received
        /// </summary>
        public delegate void MqttMsgSubscribeEventHandler(object sender, MqttMsgSubscribeEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for UNSUBSCRIBE message received
        /// </summary>
        public delegate void MqttMsgUnsubscribeEventHandler(object sender, MqttMsgUnsubscribeEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for CONNECT message received
        /// </summary>
        public delegate void MqttMsgConnectEventHandler(object sender, MqttMsgConnectEventArgs e);

        /// <summary>
        /// Delegate that defines event handler for client disconnection (DISCONNECT message or not)
        /// </summary>
        public delegate void MqttMsgDisconnectEventHandler(object sender, EventArgs e);

        /// <summary>
        /// Delegate that defines event handler for cliet/peer disconnection
        /// </summary>
        public delegate void ConnectionClosedEventHandler(object sender, EventArgs e);

        // event for PUBLISH message received
        public event MqttMsgPublishEventHandler MqttMsgPublishReceived;

        // event for published message
        public event MqttMsgPublishedEventHandler MqttMsgPublished;
        
        // event for subscribed topic
        public event MqttMsgSubscribedEventHandler MqttMsgSubscribed;
        
        // event for unsubscribed topic
        public event MqttMsgUnsubscribedEventHandler MqttMsgUnsubscribed;
        
        // event for SUBSCRIBE message received
        public event MqttMsgSubscribeEventHandler MqttMsgSubscribeReceived;
        
        // event for USUBSCRIBE message received
        public event MqttMsgUnsubscribeEventHandler MqttMsgUnsubscribeReceived;
        
        // event for CONNECT message received
        public event MqttMsgConnectEventHandler MqttMsgConnected;
        
        // event for DISCONNECT message received
        public event MqttMsgDisconnectEventHandler MqttMsgDisconnected;

        // event for peer/client disconnection
        public event ConnectionClosedEventHandler ConnectionClosed;

        #endregion

        #region Properties

        /// <summary>
        /// Connection status between client and broker
        /// </summary>
        public bool IsConnected { get; private set; }

        /// <summary>
        /// Client identifier
        /// </summary>
        public string ClientId { get; private set; }

        /// <summary>
        /// Clean session flag
        /// </summary>
        public bool CleanSession { get; private set; }

        /// <summary>
        /// Will flag
        /// </summary>
        public bool WillFlag { get; private set; }

        /// <summary>
        /// Will QOS level
        /// </summary>
        public byte WillQosLevel { get; private set; }

        /// <summary>
        /// Will topic
        /// </summary>
        public string WillTopic { get; private set; }

        /// <summary>
        /// Will message
        /// </summary>
        public string WillMessage { get; private set; }

        /// <summary>
        /// MQTT protocol version
        /// </summary>
        public MqttProtocolVersion ProtocolVersion { get; set; }

        /// <summary>
        /// MQTT Client Session
        /// </summary>
        public MqttClientSession Session { get; set; }

        /// <summary>
        /// MQTT client settings
        /// </summary>
        public MqttSettings Settings { get; private set; }

        #endregion

        /// <summary>
        /// MqttClient initialization
        /// </summary>
        /// <param name="brokerHostName">Broker Host Name or IP Address</param>
        /// <param name="brokerPort">Broker port</param>
        /// <param name="secure">>Using secure connection</param>
        /// <param name="caCert">CA certificate for secure connection</param>
        /// <param name="clientCert">Client certificate</param>
        /// <param name="sslProtocol">SSL/TLS protocol version</param>
        /// <param name="userCertificateSelectionCallback">A RemoteCertificateValidationCallback delegate responsible for validating the certificate supplied by the remote party</param>
        /// <param name="userCertificateValidationCallback">A LocalCertificateSelectionCallback delegate responsible for selecting the certificate used for authentication</param>

        /// <summary>
        /// Open client communication
        /// </summary>
        public void Open()
        {
            this.isRunning = true;
        }

        /// <summary>
        /// Close client
        /// </summary>
        public void Close()
        {
            // stop receiving thread
            this.isRunning = false;

            // unlock keep alive thread
            this.keepAliveEvent.Set();

            // close network channel
            this.channel.Close();

            this.IsConnected = false;
        }

        /// <summary>
        /// Send CONNACK message to the client (connection accepted or not)
        /// </summary>
        /// <param name="connect">CONNECT message with all client information</param>
        /// <param name="returnCode">Return code for CONNACK message</param>
        /// <param name="clientId">If not null, client id assigned by broker</param>
        /// <param name="sessionPresent">Session present on the broker</param>
        public void Connack(MqttMsgConnect connect, byte returnCode, string clientId, bool sessionPresent)
        {
            this.lastCommTime = 0;

            // create CONNACK message and ...
            MqttMsgConnack connack = new MqttMsgConnack();
            connack.ReturnCode = returnCode;
            // [v3.1.1] session present flag
            if (this.ProtocolVersion == MqttProtocolVersion.Version_3_1_1)
                connack.SessionPresent = sessionPresent;
            // ... send it to the client
            this.Send(connack);

            // connection accepted, start keep alive thread checking
            if (connack.ReturnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                // [v3.1.1] if client id isn't null, the CONNECT message has a cliend id with zero bytes length
                //          and broker assigned a unique identifier to the client
                this.ClientId = (clientId == null) ? connect.ClientId : clientId;
                this.CleanSession = connect.CleanSession;
                this.WillFlag = connect.WillFlag;
                this.WillTopic = connect.WillTopic;
                this.WillMessage = connect.WillMessage;
                this.WillQosLevel = connect.WillQosLevel;

                this.keepAlivePeriod = connect.KeepAlivePeriod * 1000; // convert in ms
                // broker has a tolerance of 1.5 specified keep alive period
                this.keepAlivePeriod += (this.keepAlivePeriod / 2);

                this.isConnectionClosing = false;
                this.IsConnected = true;
            }
            // connection refused, close TCP/IP channel
            else
            {
                this.Close();
            }
        }

        /// <summary>
        /// Send SUBACK message to the client
        /// </summary>
        /// <param name="messageId">Message Id for the SUBSCRIBE message that is being acknowledged</param>
        /// <param name="grantedQosLevels">Granted QoS Levels</param>
        public void Suback(ushort messageId, byte[] grantedQosLevels)
        {
            MqttMsgSuback suback = new MqttMsgSuback();
            suback.MessageId = messageId;
            suback.GrantedQoSLevels = grantedQosLevels;

            this.Send(suback);
        }

        /// <summary>
        /// Send UNSUBACK message to the client
        /// </summary>
        /// <param name="messageId">Message Id for the UNSUBSCRIBE message that is being acknowledged</param>
        public void Unsuback(ushort messageId)
        {
            MqttMsgUnsuback unsuback = new MqttMsgUnsuback();
            unsuback.MessageId = messageId;

            this.Send(unsuback);
        }

        /// <summary>
        /// Publish a message asynchronously
        /// </summary>
        /// <param name="topic">Message topic</param>
        /// <param name="message">Message data (payload)</param>
        /// <param name="qosLevel">QoS Level</param>
        /// <param name="retain">Retain flag</param>
        /// <returns>Message Id related to PUBLISH message</returns>
        public ushort Publish(string topic, byte[] message, byte qosLevel, bool retain)
        {
            //MqttMsgPublish publish = new MqttMsgPublish(topic, message, false, qosLevel, retain);
            //publish.MessageId = this.GetMessageId();

            //// enqueue message to publish into the inflight queue
            //// TODO FIND PROPER PLACE
            //bool enqueue = this.EnqueueInflight(publish, MqttMsgFlow.ToPublish);

            //// message enqueued
            //if (enqueue)
            //{
            //    return publish.MessageId;
            //}
            //// infligh queue full, message not enqueued
            //else
            //{
            //    throw new MqttClientException(MqttClientErrorCode.InflightQueueFull);
            //}

            throw new NotImplementedException("See todo");
        }

        public void EnqueueInternalEvent(InternalEvent internalEvent)
        {
            if (ProcessingManager != null)
            {
                eventQueue.Enqueue(internalEvent);
                ProcessingManager.EnqueueClientConnectionWithInternalEventQueueToProcess(this);
            }
        }

        public void EnqueueInflight(MqttMsgContext inflightMessageContext)
        {
            if (ProcessingManager != null)
            {
                inflightQueue.Enqueue(inflightMessageContext);
                ProcessingManager.EnqueueClientConnectionWithInflightQueueToProcess(this);
            }
        }

        public void EnqueueRawMessage(MqttRawMessage rawMessage)
        {
            if (ProcessingManager != null)
            {
                ProcessingManager.EnqueueRawMessageForProcessing(rawMessage);
            }
        }

        /// <summary>
        /// Wrapper method for raising closing connection event
        /// </summary>
        public void OnConnectionClosing()
        {
            if (!this.isConnectionClosing)
            {
                this.isConnectionClosing = true;
            }
        }

        /// <summary>
        /// Wrapper method for raising PUBLISH message received event
        /// </summary>
        /// <param name="publish">PUBLISH message received</param>
        public void OnMqttMsgPublishReceived(MqttMsgPublish publish)
        {
            if (this.MqttMsgPublishReceived != null)
            {
                this.MqttMsgPublishReceived(this, new MqttMsgPublishEventArgs(publish.Topic, publish.Message, publish.DupFlag, publish.QosLevel, publish.Retain));
            }
        }

        /// <summary>
        /// Wrapper method for raising published message event
        /// </summary>
        /// <param name="messageId">Message identifier for published message</param>
        /// <param name="isPublished">Publish flag</param>
        public void OnMqttMsgPublished(ushort messageId, bool isPublished)
        {
            if (this.MqttMsgPublished != null)
            {
                this.MqttMsgPublished(this, new MqttMsgPublishedEventArgs(messageId, isPublished));
            }
        }

        /// <summary>
        /// Wrapper method for raising subscribed topic event
        /// </summary>
        /// <param name="suback">SUBACK message received</param>
        public void OnMqttMsgSubscribed(MqttMsgSuback suback)
        {
            if (this.MqttMsgSubscribed != null)
            {
                this.MqttMsgSubscribed(this, new MqttMsgSubscribedEventArgs(suback.MessageId, suback.GrantedQoSLevels));
            }
        }

        /// <summary>
        /// Wrapper method for raising unsubscribed topic event
        /// </summary>
        /// <param name="messageId">Message identifier for unsubscribed topic</param>
        public void OnMqttMsgUnsubscribed(ushort messageId)
        {
            if (this.MqttMsgUnsubscribed != null)
            {
                this.MqttMsgUnsubscribed(this, new MqttMsgUnsubscribedEventArgs(messageId));
            }
        }

        /// <summary>
        /// Wrapper method for raising SUBSCRIBE message event
        /// </summary>
        /// <param name="messageId">Message identifier for subscribe topics request</param>
        /// <param name="topics">Topics requested to subscribe</param>
        /// <param name="qosLevels">List of QOS Levels requested</param>
        public void OnMqttMsgSubscribeReceived(ushort messageId, string[] topics, byte[] qosLevels)
        {
            if (this.MqttMsgSubscribeReceived != null)
            {
                this.MqttMsgSubscribeReceived(this, new MqttMsgSubscribeEventArgs(messageId, topics, qosLevels));
            }
        }

        /// <summary>
        /// Wrapper method for raising UNSUBSCRIBE message event
        /// </summary>
        /// <param name="messageId">Message identifier for unsubscribe topics request</param>
        /// <param name="topics">Topics requested to unsubscribe</param>
        public void OnMqttMsgUnsubscribeReceived(ushort messageId, string[] topics)
        {
            if (this.MqttMsgUnsubscribeReceived != null)
            {
                this.MqttMsgUnsubscribeReceived(this, new MqttMsgUnsubscribeEventArgs(messageId, topics));
            }
        }

        /// <summary>
        /// Wrapper method for raising CONNECT message event
        /// </summary>
        public void OnMqttMsgConnected(MqttMsgConnect connect)
        {
            if (this.MqttMsgConnected != null)
            {
                this.ProtocolVersion = (MqttProtocolVersion)connect.ProtocolVersion;
                this.MqttMsgConnected(this, new MqttMsgConnectEventArgs(connect));
            }
        }

        /// <summary>
        /// Wrapper method for raising DISCONNECT message event
        /// </summary>
        public void OnMqttMsgDisconnected()
        {
            if (this.MqttMsgDisconnected != null)
            {
                this.MqttMsgDisconnected(this, EventArgs.Empty);
            }
        }

        /// <summary>
        /// Wrapper method for peer/client disconnection
        /// </summary>
        public void OnConnectionClosed()
        {
            if (this.ConnectionClosed != null)
            {
                this.ConnectionClosed(this, EventArgs.Empty);
            }
        }

        /// <summary>
        /// Send a message
        /// </summary>
        /// <param name="msgBytes">Message bytes</param>
        public void Send(byte[] msgBytes)
        {
            try
            {
                // send message
                this.channel.Send(msgBytes);
            }
            catch (Exception e)
            {
                throw new MqttCommunicationException(e);
            }
        }

        /// <summary>
        /// Send a message
        /// </summary>
        /// <param name="msg">Message</param>
        public void Send(MqttMsgBase msg)
        {
            this.Send(msg.GetBytes((byte)this.ProtocolVersion));
        }

        /// <summary>
        /// Restore session
        /// </summary>
        private void RestoreSession()
        {
            // if not clean session
            if (!this.CleanSession)
            {
                // there is a previous session
                if (this.Session != null)
                {
                    foreach (MqttMsgContext msgContext in this.Session.InflightMessages.Values)
                    {
                        this.inflightQueue.Enqueue(msgContext);

                        // if it is a PUBLISH message to publish
                        if ((msgContext.Message.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                            && (msgContext.Flow == MqttMsgFlow.ToPublish))
                        {
                            // it's QoS 1 and we haven't received PUBACK
                            if ((msgContext.Message.QosLevel == MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE)
                                && (msgContext.State == MqttMsgState.WaitForPuback))
                            {
                                // we haven't received PUBACK, we need to resend PUBLISH message
                                msgContext.State = MqttMsgState.QueuedQos1;
                            }
                            // it's QoS 2
                            else if (msgContext.Message.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE)
                            {
                                // we haven't received PUBREC, we need to resend PUBLISH message
                                if (msgContext.State == MqttMsgState.WaitForPubrec)
                                {
                                    msgContext.State = MqttMsgState.QueuedQos2;
                                }
                                // we haven't received PUBCOMP, we need to resend PUBREL for it
                                else if (msgContext.State == MqttMsgState.WaitForPubcomp)
                                {
                                    msgContext.State = MqttMsgState.SendPubrel;
                                }
                            }
                        }
                    }
                }
                else
                {
                    // create new session
                    this.Session = new MqttClientSession(this.ClientId);
                }
            }
            // clean any previous session
            else
            {
                if (this.Session != null)
                    this.Session.Clear();
            }
        }


        /// <summary>
        /// Load a given session
        /// </summary>
        /// <param name="session">MQTT Client session to load</param>
        public void LoadSession(MqttClientSession session)
        {
            // if not clean session
            if (!this.CleanSession)
            {
                // set the session ...
                this.Session = session;
                // ... and restore it
                this.RestoreSession();
            }
        }

        /// <summary>
        /// Generate the next message identifier
        /// </summary>
        /// <returns>Message identifier</returns>
        private ushort GetMessageId()
        {
            // if 0 or max UInt16, it becomes 1 (first valid messageId)
            this.messageIdCounter = ((this.messageIdCounter % UInt16.MaxValue) != 0) ? (ushort)(this.messageIdCounter + 1) : (ushort)1;
            return this.messageIdCounter;
        }

        /// <summary>
        /// Finder class for PUBLISH message inside a queue
        /// </summary>
        internal class MqttMsgContextFinder
        {
            // PUBLISH message id
            internal ushort MessageId { get; set; }
            // message flow into inflight queue
            internal MqttMsgFlow Flow { get; set; }

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="messageId">Message Id</param>
            /// <param name="flow">Message flow inside inflight queue</param>
            internal MqttMsgContextFinder(ushort messageId, MqttMsgFlow flow)
            {
                this.MessageId = messageId;
                this.Flow = flow;
            }

            internal bool Find(object item)
            {
                MqttMsgContext msgCtx = (MqttMsgContext)item;
                return ((msgCtx.Message.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) &&
                        (msgCtx.Message.MessageId == this.MessageId) &&
                        msgCtx.Flow == this.Flow);

            }
        }
    }
}
