namespace GnatMQForAzure.Contracts
{
    using System;

    public interface ILogger
    {
        void LogException(Exception exception);

        void LogEvent(string eventName, string message);

        void LogMetric(string metricName, double value);
    }
}