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

namespace GnatMQForAzure.Communication
{
    using System;
    using System.Net;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading;

    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities.Enums;

    /// <summary>
    /// MQTT communication layer
    /// </summary>
    public class MqttTcpCommunicationLayer : IMqttCommunicationLayer
    {
        #region Constants ...

        // name for listener thread
        private const string LISTENER_THREAD_NAME = "MqttListenerThread";
        // option to accept only connection from IPv6 (or IPv4 too)
        private const int IPV6_V6ONLY = 27;

        #endregion

        #region Properties ...

        /// <summary>
        /// TCP listening port
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Secure connection (SSL/TLS)
        /// </summary>
        public bool Secure { get; private set; }

        /// <summary>
        /// X509 Server certificate
        /// </summary>
        public X509Certificate ServerCert { get; private set; }

        /// <summary>
        /// SSL/TLS protocol version
        /// </summary>
        public MqttSslProtocols Protocol { get; private set; }

        /// <summary>
        /// A RemoteCertificateValidationCallback delegate responsible for validating the certificate supplied by the remote party
        /// </summary>
        public RemoteCertificateValidationCallback UserCertificateValidationCallback { get; private set; }
        
        /// <summary>
        /// A LocalCertificateSelectionCallback delegate responsible for selecting the certificate used for authentication
        /// </summary>
        public LocalCertificateSelectionCallback UserCertificateSelectionCallback { get; private set; }

        #endregion

        // TCP listener for incoming connection requests
        private TcpListener listener;

        // TCP listener thread
        private Thread thread;
        private bool isRunning;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="port">TCP listening port</param>
        public MqttTcpCommunicationLayer(int port)
            : this(port, false, null, MqttSslProtocols.None, null, null)
        {

        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="port">TCP listening port</param>
        /// <param name="secure">Secure connection (SSL/TLS)</param>
        /// <param name="serverCert">X509 server certificate</param>
        /// <param name="protocol">SSL/TLS protocol version</param>
        /// <param name="userCertificateSelectionCallback">A RemoteCertificateValidationCallback delegate responsible for validating the certificate supplied by the remote party</param>
        /// <param name="userCertificateValidationCallback">A LocalCertificateSelectionCallback delegate responsible for selecting the certificate used for authentication</param>
        public MqttTcpCommunicationLayer(int port, bool secure, X509Certificate serverCert, MqttSslProtocols protocol,
            RemoteCertificateValidationCallback userCertificateValidationCallback,
            LocalCertificateSelectionCallback userCertificateSelectionCallback)
        {
            if (secure && serverCert == null)
                throw new ArgumentException("Secure connection requested but no server certificate provided");

            this.Port = port;
            this.Secure = secure;
            this.ServerCert = serverCert;
            this.Protocol = protocol;
            this.UserCertificateValidationCallback = userCertificateValidationCallback;
            this.UserCertificateSelectionCallback = userCertificateSelectionCallback;
        }

        #region IMqttCommunicationLayer ...

        // client connected event
        public event MqttClientConnectedEventHandler ClientConnected;

        /// <summary>
        /// Start communication layer listening
        /// </summary>
        public void Start()
        {
            this.isRunning = true;

            // create and start listener thread
            this.thread = new Thread(this.ListenerThread);
            this.thread.Name = LISTENER_THREAD_NAME;
            this.thread.Start();
        }

        /// <summary>
        /// Stop communication layer listening
        /// </summary>
        public void Stop()
        {
            this.isRunning = false;

            this.listener.Stop();

            // wait for thread
            this.thread.Join();
        }

        #endregion

        /// <summary>
        /// Listener thread for incoming connection requests
        /// </summary>
        private void ListenerThread()
        {
            // create listener...
            this.listener = new TcpListener(IPAddress.IPv6Any, this.Port);
            // set socket option 27 (IPV6_V6ONLY) to false to accept also connection on IPV4 (not only IPV6 as default)
            this.listener.Server.SetSocketOption(SocketOptionLevel.IPv6, (SocketOptionName)IPV6_V6ONLY, false);
            // ...and start it
            this.listener.Start();

            while (this.isRunning)
            {
                try
                {
                    // blocking call to wait for client connection
                    Socket socketClient = this.listener.AcceptSocket();

                    // manage socket client connected
                    if (socketClient.Connected)
                    {
                        // create network channel to accept connection request
                        IMqttNetworkChannel channel = null;
                        if (this.Secure)
                        {
                            channel = new MqttNetworkChannel(socketClient, this.Secure, this.ServerCert, this.Protocol, this.UserCertificateValidationCallback, this.UserCertificateSelectionCallback);

                        }
                        else
                        {
                            channel = new MqttNetworkChannel(socketClient);
                        }

                        channel.Accept();

                        // handling channel for connected client
                        //MqttClientConnection clientConnection = new MqttClientConnection(channel);
                        //// raise client raw connection event
                        //this.OnClientConnected(clientConnection);
                    }
                }
                catch (Exception)
                {
                    if (!this.isRunning)
                        return;
                }
            }
        }

        /// <summary>
        /// Raise client connected event
        /// </summary>
        /// <param name="e">Event args</param>
        private void OnClientConnected(MqttClientConnection clientConnection)
        {
            if (this.ClientConnected != null)
                this.ClientConnected(this, new MqttClientConnectedEventArgs(clientConnection));
        }
    }
}
