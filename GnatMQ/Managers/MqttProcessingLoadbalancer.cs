namespace GnatMQForAzure.Managers
{
    using System.Collections.Concurrent;
    using System.Security.Policy;
    using System.Threading;

    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;
    using GnatMQForAzure.Utility;

    public class MqttProcessingLoadbalancer : IMqttRunnable, IMqttClientConnectionStarter, IPeriodicallyLoggable
    {
        private readonly ILogger logger;

        private readonly MqttClientConnectionProcessingManager[] processingManagers;

        private readonly AutoResetEvent loadbalanceAwaitHandler;

        private int indexOfProcessingManagerToGetNextConnection;

        private int numberOfConnectionsLoadbalanced;

        private bool isRunning;

        public MqttProcessingLoadbalancer(ILogger logger, MqttClientConnectionProcessingManager[] processingManagers)
        {
            this.logger = logger;
            this.processingManagers = processingManagers;
            this.loadbalanceAwaitHandler = new AutoResetEvent(false);
        }

        public void Start()
        {
            this.isRunning = true;
            Fx.StartThread(Loadbalancer);
        }

        public void Stop()
        {
            this.isRunning = false;
        }

        public void PeriodicLogging()
        {
            var loadbalancedCopy = numberOfConnectionsLoadbalanced;
            logger.LogMetric(this, LoggerConstants.NumberOfConnectionsLoadbalanced, loadbalancedCopy);
            Interlocked.Add(ref numberOfConnectionsLoadbalanced, -loadbalancedCopy);
        }

        public void OpenClientConnection(MqttClientConnection clientConnection)
        {
            processingManagers[indexOfProcessingManagerToGetNextConnection].OpenClientConnection(clientConnection);
            loadbalanceAwaitHandler.Set();
            Interlocked.Increment(ref numberOfConnectionsLoadbalanced);
        }

        private void Loadbalancer()
        {
            while (isRunning)
            {
                loadbalanceAwaitHandler.WaitOne();

                var loadbalancerWithTheLeastConnections = 0;
                var leastNumberOfConnections = int.MaxValue;
                for (var i = 0; i < processingManagers.Length; i++)
                {
                    if (processingManagers[i].AssignedClients < leastNumberOfConnections)
                    {
                        leastNumberOfConnections = processingManagers[i].AssignedClients;
                        loadbalancerWithTheLeastConnections = i;
                    }
                }

                indexOfProcessingManagerToGetNextConnection = loadbalancerWithTheLeastConnections;
            }
        }
    }
}