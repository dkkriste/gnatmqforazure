namespace GnatMQForAzure.Managers
{
    using System;
    using System.Threading;

    public class MqttClientConnectionTimeoutManager
    {
        /// <summary>
        /// Thread for handling keep alive message
        /// </summary>
        private void KeepAliveThread(MqttClientConnection clientConnection)
        {
            int delta = 0;
            int wait = clientConnection.keepAlivePeriod;

            // create event to signal that current thread is end
            clientConnection.keepAliveEventEnd = new AutoResetEvent(false);

            while (clientConnection.isRunning)
            {

                // waiting...
                clientConnection.keepAliveEvent.WaitOne(wait);

                if (clientConnection.isRunning)
                {
                    delta = Environment.TickCount - clientConnection.lastCommTime;

                    // if timeout exceeded ...
                    if (delta >= clientConnection.keepAlivePeriod)
                    {
                        // client must close connection
                        clientConnection.OnConnectionClosing();
                    }
                    else
                    {
                        // update waiting time
                        wait = clientConnection.keepAlivePeriod - delta;
                    }
                }
            }

            // signal thread end
            clientConnection.keepAliveEventEnd.Set();
        }
    }
}