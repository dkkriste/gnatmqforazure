namespace GnatMQForAzure.Contracts
{
    using System;

    public interface ILogger
    {
        void LogException(object sender, Exception exception);

        void LogEvent(object sender, string eventName, string message);

        void LogMetric(object sender, string metricName, double value);
    }
}