namespace GnatMQWorkerRole
{
    using System;
    using System.Diagnostics;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    using GnatMQForAzure;
    using GnatMQForAzure.Contracts;
    using GnatMQForAzure.Entities;

    using Microsoft.WindowsAzure.ServiceRuntime;

    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        private ILogger logger;

        private MqttBroker broker;

        private MqttOptions options;

        public override void Run()
        {
            Trace.TraceInformation("GnatMQWorkerRole is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = int.MaxValue;

            logger = new ApplicationInsightsLogger();
            var options = new MqttOptions
            {
                ConnectionsPrProcessingManager = 1024,
                EndPoint = new IPEndPoint(IPAddress.Any, 1883),
                IndividualMessageBufferSize = 8192,
                NumberOfAcceptSaea = 512,
                MaxConnections = 32768,
                InitialNumberOfRawMessages = 1024,
                NumberOfSendBuffers = 32768,
                ReadAndSendBufferSize = 8192
            };

            broker = new MqttBroker(logger, options);

            broker.Start();

            bool result = base.OnStart();

            Trace.TraceInformation("GnatMQWorkerRole has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("GnatMQWorkerRole is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            broker.Stop();

            base.OnStop();

            Trace.TraceInformation("GnatMQWorkerRole has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            var loggingPeriod = new TimeSpan(0, 1, 0);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    broker.PeriodicLogging();
                    await Task.Delay(loggingPeriod, cancellationToken);
                }
                catch (Exception exception)
                {
                    logger.LogException(this, exception);
                }
            }
        }
    }
}
