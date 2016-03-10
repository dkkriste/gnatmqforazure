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
            int wait = clientConnection.KeepAlivePeriod;

            // create event to signal that current thread is end
            clientConnection.KeepAliveEventEnd = new AutoResetEvent(false);

            while (clientConnection.IsRunning)
            {
                // waiting...
                clientConnection.KeepAliveEvent.WaitOne(wait);

                if (clientConnection.IsRunning)
                {
                    delta = Environment.TickCount - clientConnection.LastCommunicationTime;

                    // if timeout exceeded ...
                    if (delta >= clientConnection.KeepAlivePeriod)
                    {
                        // client must close connection
                        clientConnection.OnConnectionClosing();
                    }
                    else
                    {
                        // update waiting time
                        wait = clientConnection.KeepAlivePeriod - delta;
                    }
                }
            }

            // signal thread end
            clientConnection.KeepAliveEventEnd.Set();
        }
    }
}