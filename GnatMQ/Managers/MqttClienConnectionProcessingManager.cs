namespace GnatMQForAzure.Managers
{
    using System;
    using System.Collections.Concurrent;

    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Utility;

    public class MqttClienConnectionProcessingManager : IMqttRunnable
    {
        private readonly ILogger logger;

        private readonly BlockingCollection<MqttRawMessage> rawMessageQueue;

        private readonly BlockingCollection<MqttClientConnection> clientConnectionsWithInflightQueuesToProcess;

        private readonly BlockingCollection<MqttClientConnection> clientConnectionsWithInternalEventQueuesToProcess;

        private bool isRunning;

        public MqttClienConnectionProcessingManager(ILogger logger)
        {
            this.logger = logger;
            rawMessageQueue = new BlockingCollection<MqttRawMessage>();
            clientConnectionsWithInflightQueuesToProcess = new BlockingCollection<MqttClientConnection>();
            clientConnectionsWithInternalEventQueuesToProcess = new BlockingCollection<MqttClientConnection>();
        }

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

        private void ProcessRawMessageQueue()
        {
            while (isRunning)
            {
                try
                {
                    var rawMessage = rawMessageQueue.Take();
                    //TODO
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
                    //TODO
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
                    //TODO
                }
                catch (Exception exception)
                {
                    logger.LogException(exception);
                }
            }
        }

        private void CheckConnect(MqttClientConnection clientConnection)
        {
            if ((clientConnection.eventQueue.Count == 0) && !clientConnection.isConnectionClosing)
            {
                // broker need to receive the first message (CONNECT)
                // within a reasonable amount of time after TCP/IP connection
                if (!clientConnection.IsConnected)
                {
                    // wait on receiving message from client with a connection timeout
                    if (!clientConnection.receiveEventWaitHandle.WaitOne(clientConnection.Settings.TimeoutOnConnection))
                    {
                        // client must close connection
                        clientConnection.Close();

                        // client raw disconnection
                        clientConnection.OnConnectionClosed();
                    }
                }
                else
                {
                    // wait on receiving message from client
                    clientConnection.receiveEventWaitHandle.WaitOne();
                }
            }

        }
    }
}