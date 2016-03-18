namespace GnatMQServer
{
    using System;

    using GnatMQForAzure.Contracts;

    public class ConsoleLogger : ILogger
    {
        public void LogException(object sender, Exception exception)
        {
            Console.WriteLine(sender.GetType().Name + ": " + exception);
        }

        public void LogEvent(object sender, string eventName, string message)
        {
            Console.WriteLine(sender.GetType().Name + ": " + eventName + " " + message);
        }

        public void LogMetric(object sender, string metricName, double value)
        {
            Console.WriteLine(sender.GetType().Name + ": " + metricName + " " + value);
        }
    }
}