namespace GnatMQForAzure.Communication
{
    using System;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;

    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;

    // Inspired by http://www.codeproject.com/Articles/83102/C-SocketAsyncEventArgs-High-Performance-Socket-Cod
    public class MqttAsyncTcpSocketListener : IMqttRunnable
    {
        private readonly SocketAsyncEventArgsPool poolOfAcceptEventArgs;

        private Socket listenSocket;

        private bool isRunning;

        public MqttAsyncTcpSocketListener(MqttOptions options)
        {
            poolOfAcceptEventArgs = new SocketAsyncEventArgsPool(options.NumberOfAcceptSaea);
            for (var i = 0; i < options.NumberOfAcceptSaea; i++)
            {
                this.poolOfAcceptEventArgs.Push(CreateNewSaeaForAccept());
            }
        }

        public void Start()
        {
            isRunning = true;
        }

        public void Stop()
        {
            isRunning = false;
        }

        private SocketAsyncEventArgs CreateNewSaeaForAccept()
        {
            SocketAsyncEventArgs acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArgCompleted;
            return acceptEventArg;
        }

        private void StartListen(IPEndPoint endPoint)
        {
            // create the socket which listens for incoming connections
            listenSocket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listenSocket.Bind(endPoint);
            listenSocket.Listen(1024);
            StartAccept();
        }

        private void StartAccept()
        {
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

            // TODO 
            SocketAsyncEventArgs receiveSendEventArgs = null;

            receiveSendEventArgs.AcceptSocket = acceptEventArgs.AcceptSocket;

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
