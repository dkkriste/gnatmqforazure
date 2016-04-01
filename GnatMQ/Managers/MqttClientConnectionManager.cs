namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;
    using System.Net.Sockets;
    using System.Threading;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Utility;

    public class MqttClientConnectionManager : IMqttClientConnectionManager 
    {
        private readonly ILogger logger;

        private readonly ConcurrentStack<MqttClientConnection> unconnectedClientPool;

        private int numberOfConnectionsGotten;

        private int numberOfConnectionsReturned;

        public MqttClientConnectionManager(ILogger logger, MqttOptions options, MqttAsyncTcpReceiver receiver)
        {
            this.logger = logger;
            numberOfConnectionsGotten = 0;
            numberOfConnectionsReturned = 0;

            unconnectedClientPool = new ConcurrentStack<MqttClientConnection>();
            var readSocketBufferManager = new BufferManager(options.MaxConnections, options.ReadAndSendBufferSize);

            for (var i = 0; i < options.MaxConnections; i++)
            {
                var receiveSocketEventArg = new SocketAsyncEventArgs();
                readSocketBufferManager.SetBuffer(receiveSocketEventArg);
                receiveSocketEventArg.Completed += receiver.ReceiveCompleted;
                var clientConnection = new MqttClientConnection(receiveSocketEventArg);

                unconnectedClientPool.Push(clientConnection);
            }
        }

        public MqttClientConnection GetConnection()
        {
            MqttClientConnection clientConnection;
            if (unconnectedClientPool.TryPop(out clientConnection))
            {
                Interlocked.Increment(ref numberOfConnectionsGotten);
                return clientConnection;
            }

            var exception = new Exception("Maximum number of connections reached");
            logger.LogException(this, exception);

            throw exception;
        }

        public void ReturnConnection(MqttClientConnection clientConnection)
        {
            try
            {
                clientConnection.ResetSocket();
            }
            catch (Exception)
            {
            }

            clientConnection.Reset();

            Interlocked.Increment(ref numberOfConnectionsReturned);

            unconnectedClientPool.Push(clientConnection);
        }

        public void PeriodicLogging()
        {
            var numberOfConnectionsGottenCopy = numberOfConnectionsGotten;
            var numberOfConnectionsReturnedCopy = numberOfConnectionsReturned;

            logger.LogMetric(this, LoggerConstants.NumberOfClientConnectionsGotten, numberOfConnectionsGottenCopy);
            logger.LogMetric(this, LoggerConstants.NumberOfClientConnectionsReturned, numberOfConnectionsReturnedCopy);
            logger.LogMetric(this, LoggerConstants.NumberOfClientConnectionsAvailable, unconnectedClientPool.Count);

            Interlocked.Add(ref numberOfConnectionsGotten, -numberOfConnectionsGottenCopy);
            Interlocked.Add(ref numberOfConnectionsReturned, -numberOfConnectionsReturnedCopy);
        }
    }
}