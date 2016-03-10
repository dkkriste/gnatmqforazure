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

        // event for published message
        public event MqttMsgPublishedEventHandler MqttMsgPublished;
        
        // event for subscribed topic
        public event MqttMsgSubscribedEventHandler MqttMsgSubscribed;
        
        // event for unsubscribed topic
        public event MqttMsgUnsubscribedEventHandler MqttMsgUnsubscribed;
        
        // event for DISCONNECT message received
        public event MqttMsgDisconnectEventHandler MqttMsgDisconnected;

        #endregion

        #region Properties

        /// <summary>
        /// Connection status between client and broker
        /// </summary>
        public bool IsConnected { get; set; }

        /// <summary>
        /// Client identifier
        /// </summary>
        public string ClientId { get; set; }

        /// <summary>
        /// Clean session flag
        /// </summary>
        public bool CleanSession { get; set; }

        /// <summary>
        /// Will flag
        /// </summary>
        public bool WillFlag { get; set; }

        /// <summary>
        /// Will QOS level
        /// </summary>
        public byte WillQosLevel { get; set; }

        /// <summary>
        /// Will topic
        /// </summary>
        public string WillTopic { get; set; }

        /// <summary>
        /// Will message
        /// </summary>
        public string WillMessage { get; set; }

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

        public void OnConnectionClosing()
        {
            if (!this.isConnectionClosing)
            {
                this.isConnectionClosing = true;
            }
        }

        public void OnMqttMsgConnected(MqttMsgConnect connect)
        {
            if (ProcessingManager != null)
            {
                ProcessingManager.OnMqttMsgConnected(this, connect);
            }
        }

        public void OnMqttMsgDisconnected()
        {
            if (this.MqttMsgDisconnected != null)
            {
                this.MqttMsgDisconnected(this, EventArgs.Empty);
            }
        }

        public void OnConnectionClosed()
        {
            if (ProcessingManager != null)
            {
                ProcessingManager.OnConnectionClosed(this);
            }
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
        /// Publish a message asynchronously
        /// </summary>
        /// <param name="topic">Message topic</param>
        /// <param name="message">Message data (payload)</param>
        /// <param name="qosLevel">QoS Level</param>
        /// <param name="retain">Retain flag</param>
        /// <returns>Message Id related to PUBLISH message</returns>
        public ushort Publish(string topic, byte[] message, byte qosLevel, bool retain)
        {
            MqttMsgPublish publish = new MqttMsgPublish(topic, message, false, qosLevel, retain);
            publish.MessageId = this.GetMessageId();

            // enqueue message to publish into the inflight queue
            // TODO FIND PROPER PLACE
            bool enqueue = this.EnqueueInflight(publish, MqttMsgFlow.ToPublish);

            // message enqueued
            if (enqueue)
            {
                return publish.MessageId;
            }
            // infligh queue full, message not enqueued
            else
            {
                throw new MqttClientException(MqttClientErrorCode.InflightQueueFull);
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
