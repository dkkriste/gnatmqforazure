namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;
    using System.Net.Sockets;

    using GnatMQForAzure.Communication;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;

    public class MqttClientConnectionManager : IMqttClientConnectionManager
    {
        private readonly ConcurrentDictionary<Guid, MqttClientConnection> connectedClients;

        private readonly ConcurrentStack<MqttClientConnection> unconnectedClients;

        private readonly BufferManager readSocketBufferManager;

        public MqttClientConnectionManager(MqttOptions options, MqttAsyncTcpReceiver receiver)
        {
            connectedClients = new ConcurrentDictionary<Guid, MqttClientConnection>();
            unconnectedClients = new ConcurrentStack<MqttClientConnection>();
            readSocketBufferManager = new BufferManager(1024, 8192);

            for (var i = 0; i < options.MaxConnections; i++)
            {
                var receiveSocketEventArg = new SocketAsyncEventArgs();
                this.readSocketBufferManager.SetBuffer(receiveSocketEventArg);
                receiveSocketEventArg.Completed += receiver.ReceiveCompleted;
                var clientConnection = new MqttClientConnection(receiveSocketEventArg);

                unconnectedClients.Push(clientConnection);
            }
        }

        public MqttClientConnection GetConnection()
        {
            MqttClientConnection clientConnection;
            if (unconnectedClients.TryPop(out clientConnection))
            {
                return clientConnection;
            }

            throw new Exception("Maximum number of connections reached");
        }

        public void ReturnConnection(MqttClientConnection clientConnection)
        {
            //TODO reset connection
            unconnectedClients.Push(clientConnection);
        }
    }
}