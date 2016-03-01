namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;
    using System.Net.Sockets;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Entities;

    public class MqttClientConnectionManager
    {
        private readonly MqttAsyncTcpReceiver receiver;

        private readonly ConcurrentDictionary<Guid, MqttClientConnection> connectedClients;

        private readonly ConcurrentStack<MqttClientConnection> unconnectedClients;

        private readonly BufferManager readSocketBufferManager;

        public MqttClientConnectionManager(MqttOptions options, MqttAsyncTcpReceiver receiver)
        {
            this.receiver = receiver;
            connectedClients = new ConcurrentDictionary<Guid, MqttClientConnection>();
            unconnectedClients = new ConcurrentStack<MqttClientConnection>();
            readSocketBufferManager = new BufferManager(1024, 8192);

            for (var i = 0; i < options.NumberOfConnections; i++)
            {
                var receiveSocketEventArg = new SocketAsyncEventArgs();
                this.readSocketBufferManager.SetBuffer(receiveSocketEventArg);
                receiveSocketEventArg.Completed += receiver.ReceiveCompleted;
                var clientConnection = new MqttClientConnection(receiveSocketEventArg);

                unconnectedClients.Push(clientConnection);
            }
        }

        public MqttClientConnection GetAvailableConnection()
        {
            MqttClientConnection clientConnection;
            if (unconnectedClients.TryPop(out clientConnection))
            {
                return clientConnection;
            }

            throw new Exception("Maximum number of connections reached");
        }

        public void ReturnConnectionToPool(MqttClientConnection clientConnection)
        {
            //TODO reset connection
            unconnectedClients.Push(clientConnection);
        }
    }
}