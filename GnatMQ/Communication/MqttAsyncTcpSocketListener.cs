namespace GnatMQForAzure.Communication
{
    using System.Net.Sockets;

    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;

    // Inspired by http://www.codeproject.com/Articles/83102/C-SocketAsyncEventArgs-High-Performance-Socket-Cod
    public class MqttAsyncTcpSocketListener : IMqttRunnable
    {
        private readonly IMqttClientConnectionStarter clientConnectionStarter;

        private readonly IMqttClientConnectionManager connectionManager;

        private readonly SocketAsyncEventArgsPool poolOfAcceptEventArgs;

        private readonly Socket listenSocket;

        private bool isRunning;

        public MqttAsyncTcpSocketListener(IMqttClientConnectionStarter clientConnectionStarter, IMqttClientConnectionManager connectionManager, MqttOptions options)
        {
            this.clientConnectionStarter = clientConnectionStarter;
            this.connectionManager = connectionManager;
            poolOfAcceptEventArgs = new SocketAsyncEventArgsPool();
            for (var i = 0; i < options.NumberOfAcceptSaea; i++)
            {
                this.poolOfAcceptEventArgs.Push(CreateNewSaeaForAccept());
            }

            // create the socket which listens for incoming connections
            listenSocket = new Socket(options.EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listenSocket.Bind(options.EndPoint);
        }

        public void Start()
        {
            isRunning = true;
            listenSocket.Listen(1024);
            StartAccept();
        }

        public void Stop()
        {
            isRunning = false;
            listenSocket.Close();
        }

        private SocketAsyncEventArgs CreateNewSaeaForAccept()
        {
            SocketAsyncEventArgs acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArgCompleted;
            return acceptEventArg;
        }

        private void StartAccept()
        {
            if (!isRunning)
            {
                return;
            }

            SocketAsyncEventArgs acceptEventArg;
            if (this.poolOfAcceptEventArgs.Count > 1)
            {
                try
                {
                    acceptEventArg = this.poolOfAcceptEventArgs.Pop();
                }
                catch
                {
                    acceptEventArg = CreateNewSaeaForAccept();
                }
            }
            else
            {
                acceptEventArg = CreateNewSaeaForAccept();
            }

            bool willRaiseEvent = listenSocket.AcceptAsync(acceptEventArg);
            if (!willRaiseEvent)
            {
                ProcessAccept(acceptEventArg);
            }
        }

        private void AcceptEventArgCompleted(object sender, SocketAsyncEventArgs e)
        {
            ProcessAccept(e);
        }

        private void ProcessAccept(SocketAsyncEventArgs acceptEventArgs)
        {
            if (acceptEventArgs.SocketError != SocketError.Success)
            {
                StartAccept();
                HandleBadAccept(acceptEventArgs);
                return;
            }

            StartAccept();

            var clientConnection = connectionManager.GetConnection();
            if (clientConnection != null)
            {
                clientConnection.ReceiveSocketAsyncEventArgs.AcceptSocket = acceptEventArgs.AcceptSocket;
                clientConnectionStarter.OpenClientConnection(clientConnection);
            }

            acceptEventArgs.AcceptSocket = null;
            this.poolOfAcceptEventArgs.Push(acceptEventArgs);
        }

        private void HandleBadAccept(SocketAsyncEventArgs acceptEventArgs)
        {
            acceptEventArgs.AcceptSocket.Close();
            poolOfAcceptEventArgs.Push(acceptEventArgs);
        }
    }
}
