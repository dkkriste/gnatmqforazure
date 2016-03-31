namespace GnatMQWorkerRole
{
    using System;

    using GnatMQForAzure.Contracts;

    public class ApplicationInsightsLogger : ILogger
    {
        public void LogException(object sender, Exception exception)
        {
            throw new NotImplementedException();
        }

        public void LogEvent(object sender, string eventName, string message)
        {
            throw new NotImplementedException();
        }

        public void LogMetric(object sender, string metricName, double value)
        {
            throw new NotImplementedException();
        }
    }
}