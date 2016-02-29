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

    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Enums;
    using GnatMQForAzure.Utility;

    /// <summary>
    /// Channel to communicate over the network
    /// </summary>
    public class MqttNetworkChannel : IMqttNetworkChannel
    {
        private readonly RemoteCertificateValidationCallback userCertificateValidationCallback;
        private readonly LocalCertificateSelectionCallback userCertificateSelectionCallback;
        // remote host information

        // socket for communication
        private Socket socket;
        // using SSL
        private bool secure;

        // CA certificate (on client)
        private X509Certificate caCert;
        // Server certificate (on broker)
        private X509Certificate serverCert;
        // client certificate (on client)
        private X509Certificate clientCert;

        // SSL/TLS protocol version
        private MqttSslProtocols sslProtocol;

        /// <summary>
        /// Remote host name
        /// </summary>
        public string RemoteHostName { get; }

        /// <summary>
        /// Remote IP address
        /// </summary>
        public IPAddress RemoteIpAddress { get; }

        /// <summary>
        /// Remote port
        /// </summary>
        public int RemotePort { get; }

        // SSL stream
        private SslStream sslStream;
        private NetworkStream netStream;

        /// <summary>
        /// Data available on the channel
        /// </summary>
        public bool DataAvailable
        {
            get
            {
                if (secure)
                    return this.netStream.DataAvailable;
                else
                    return (this.socket.Available > 0);

            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="socket">Socket opened with the client</param>
        public MqttNetworkChannel(Socket socket)
            : this(socket, false, null, MqttSslProtocols.None, null, null)
        {
        }
        
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="socket">Socket opened with the client</param>
        /// <param name="secure">Secure connection (SSL/TLS)</param>
        /// <param name="serverCert">Server X509 certificate for secure connection</param>
        /// <param name="sslProtocol">SSL/TLS protocol version</param>
        /// <param name="userCertificateSelectionCallback">A RemoteCertificateValidationCallback delegate responsible for validating the certificate supplied by the remote party</param>
        /// <param name="userCertificateValidationCallback">A LocalCertificateSelectionCallback delegate responsible for selecting the certificate used for authentication</param>
        public MqttNetworkChannel(Socket socket, bool secure, X509Certificate serverCert, MqttSslProtocols sslProtocol,
            RemoteCertificateValidationCallback userCertificateValidationCallback,
            LocalCertificateSelectionCallback userCertificateSelectionCallback)
        {
            this.socket = socket;
            this.secure = secure;
            this.serverCert = serverCert;
            this.sslProtocol = sslProtocol;
            this.userCertificateValidationCallback = userCertificateValidationCallback;
            this.userCertificateSelectionCallback = userCertificateSelectionCallback;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="remoteHostName">Remote Host name</param>
        /// <param name="remotePort">Remote port</param>
        public MqttNetworkChannel(string remoteHostName, int remotePort)
            : this(remoteHostName, remotePort, false, null, null, MqttSslProtocols.None, null, null)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="remoteHostName">Remote Host name</param>
        /// <param name="remotePort">Remote port</param>
        /// <param name="secure">Using SSL</param>
        /// <param name="caCert">CA certificate</param>
        /// <param name="clientCert">Client certificate</param>
        /// <param name="sslProtocol">SSL/TLS protocol version</param>
        /// <param name="userCertificateSelectionCallback">A RemoteCertificateValidationCallback delegate responsible for validating the certificate supplied by the remote party</param>
        /// <param name="userCertificateValidationCallback">A LocalCertificateSelectionCallback delegate responsible for selecting the certificate used for authentication</param>
        public MqttNetworkChannel(string remoteHostName, int remotePort, bool secure, X509Certificate caCert, X509Certificate clientCert, MqttSslProtocols sslProtocol,
            RemoteCertificateValidationCallback userCertificateValidationCallback,
            LocalCertificateSelectionCallback userCertificateSelectionCallback)
        {
            IPAddress remoteIpAddress = null;
            try
            {
                // check if remoteHostName is a valid IP address and get it
                remoteIpAddress = IPAddress.Parse(remoteHostName);
            }
            catch
            {
            }

            // in this case the parameter remoteHostName isn't a valid IP address
            if (remoteIpAddress == null)
            {
                IPHostEntry hostEntry = Dns.GetHostEntry(remoteHostName);
                if ((hostEntry != null) && (hostEntry.AddressList.Length > 0))
                {
                    // check for the first address not null
                    // it seems that with .Net Micro Framework, the IPV6 addresses aren't supported and return "null"
                    int i = 0;
                    while (hostEntry.AddressList[i] == null) i++;
                    remoteIpAddress = hostEntry.AddressList[i];
                }
                else
                {
                    throw new Exception("No address found for the remote host name");
                }
            }

            this.RemoteHostName = remoteHostName;
            this.RemoteIpAddress = remoteIpAddress;
            this.RemotePort = remotePort;
            this.secure = secure;
            this.caCert = caCert;
            this.clientCert = clientCert;
            this.sslProtocol = sslProtocol;
            this.userCertificateValidationCallback = userCertificateValidationCallback;
            this.userCertificateSelectionCallback = userCertificateSelectionCallback;
        }

        /// <summary>
        /// Connect to remote server
        /// </summary>
        public void Connect()
        {
            this.socket = new Socket(this.RemoteIpAddress.GetAddressFamily(), SocketType.Stream, ProtocolType.Tcp);
            // try connection to the broker
            this.socket.Connect(new IPEndPoint(this.RemoteIpAddress, this.RemotePort));

            // secure channel requested
            if (secure)
            {
                // create SSL stream
                this.netStream = new NetworkStream(this.socket);
                this.sslStream = new SslStream(this.netStream, false, this.userCertificateValidationCallback, this.userCertificateSelectionCallback);

                // server authentication (SSL/TLS handshake)
                X509CertificateCollection clientCertificates = null;
                // check if there is a client certificate to add to the collection, otherwise it's null (as empty)
                if (this.clientCert != null)
                    clientCertificates = new X509CertificateCollection(new X509Certificate[] { this.clientCert });

                this.sslStream.AuthenticateAsClient(this.RemoteHostName,
                    clientCertificates,
                    MqttSslUtility.ToSslPlatformEnum(this.sslProtocol),
                    false);
                
            }
        }

        /// <summary>
        /// Send data on the network channel
        /// </summary>
        /// <param name="buffer">Data buffer to send</param>
        /// <returns>Number of byte sent</returns>
        public int Send(byte[] buffer)
        {
            if (this.secure)
            {
                this.sslStream.Write(buffer, 0, buffer.Length);
                this.sslStream.Flush();
                return buffer.Length;
            }
            else
                return this.socket.Send(buffer, 0, buffer.Length, SocketFlags.None);
        }

        /// <summary>
        /// Receive data from the network
        /// </summary>
        /// <param name="buffer">Data buffer for receiving data</param>
        /// <returns>Number of bytes received</returns>
        public int Receive(byte[] buffer)
        {
            if (this.secure)
            {
                // read all data needed (until fill buffer)
                int idx = 0, read = 0;
                while (idx < buffer.Length)
                {
                    // fixed scenario with socket closed gracefully by peer/broker and
                    // Read return 0. Avoid infinite loop.
                    read = this.sslStream.Read(buffer, idx, buffer.Length - idx);
                    if (read == 0)
                        return 0;
                    idx += read;
                }
                return buffer.Length;
            }
            else
            {
                // read all data needed (until fill buffer)
                int idx = 0, read = 0;
                while (idx < buffer.Length)
                {
                    // fixed scenario with socket closed gracefully by peer/broker and
                    // Read return 0. Avoid infinite loop.
                    read = this.socket.Receive(buffer, idx, buffer.Length - idx, SocketFlags.None);
                    if (read == 0)
                        return 0;
                    idx += read;
                }
                return buffer.Length;
            }
        }

        /// <summary>
        /// Receive data from the network channel with a specified timeout
        /// </summary>
        /// <param name="buffer">Data buffer for receiving data</param>
        /// <param name="timeout">Timeout on receiving (in milliseconds)</param>
        /// <returns>Number of bytes received</returns>
        public int Receive(byte[] buffer, int timeout)
        {
            // check data availability (timeout is in microseconds)
            if (this.socket.Poll(timeout * 1000, SelectMode.SelectRead))
            {
                return this.Receive(buffer);
            }
            else
            {
                return 0;
            }
        }

        /// <summary>
        /// Close the network channel
        /// </summary>
        public void Close()
        {
            if (this.secure)
            {
                this.netStream.Close();
                this.sslStream.Close();
            }
            this.socket.Close();
        }

        /// <summary>
        /// Accept connection from a remote client
        /// </summary>
        public void Accept()
        {
            // secure channel requested
            if (secure)
            {

                this.netStream = new NetworkStream(this.socket);
                this.sslStream = new SslStream(this.netStream, false, this.userCertificateValidationCallback, this.userCertificateSelectionCallback);

                this.sslStream.AuthenticateAsServer(this.serverCert, false, MqttSslUtility.ToSslPlatformEnum(this.sslProtocol), false);
            }
        }
    }
}
