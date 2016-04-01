namespace GnatMQForAzure.Managers
{
    using System;
    using System.Threading;

    using GnatMQForAzure.Messages;
    using GnatMQForAzure.Utility;

    public static class MqttKeepAliveManager
    {
        private static bool isRunning;

        public static void Start()
        {
            isRunning = true;
            Fx.StartThread(KeepAliveThread);
        }

        public static void Stop()
        {
            isRunning = false;
        }

        private static void KeepAliveThread()
        {
            var sleepPeriod = new TimeSpan(0, 0, MqttMsgConnect.KEEP_ALIVE_PERIOD_DEFAULT);
            while (isRunning)
            {
                try
                {
                    var now = Environment.TickCount;
                    foreach (var clientConnection in MqttBroker.GetAllConnectedClients())
                    {
                        if (clientConnection.IsRunning && clientConnection.IsConnected)
                        {
                            var delta = now - clientConnection.LastCommunicationTime;
                            if (delta >= clientConnection.KeepAlivePeriod)
                            {
                                clientConnection.OnConnectionClosed();
                            }
                        }
                    }

                    Thread.Sleep(sleepPeriod);
                }
                catch (Exception)
                {
                }
            }
        }
    }
}