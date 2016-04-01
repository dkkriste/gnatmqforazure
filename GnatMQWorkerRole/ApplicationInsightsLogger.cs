namespace GnatMQWorkerRole
{
    using System;
    using System.Collections.Generic;

    using GnatMQForAzure.Contracts;

    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.Azure;
    using Microsoft.WindowsAzure;

    public class ApplicationInsightsLogger : ILogger
    {
        private static TelemetryClient applicationInsightsClient;

        public ApplicationInsightsLogger()
        {
            if (applicationInsightsClient == null)
            {
                TelemetryConfiguration.Active.InstrumentationKey = CloudConfigurationManager.GetSetting("Telemetry.AI.InstrumentationKey");
                applicationInsightsClient = new TelemetryClient();
            }
        }

        public void LogException(object sender, Exception exception)
        {
            var properties = new Dictionary<string, string>();
            properties.Add("Sender", sender.GetType().Name);
            applicationInsightsClient.TrackException(exception, properties);
        }

        public void LogEvent(object sender, string eventName, string message)
        {
            var properties = new Dictionary<string, string>();
            properties.Add("Sender", sender.GetType().Name);
            properties.Add("Message", message);
            applicationInsightsClient.TrackEvent(eventName, properties);
        }

        public void LogMetric(object sender, string metricName, double value)
        {
            var properties = new Dictionary<string, string>();
            properties.Add("Sender", sender.GetType().Name);
            applicationInsightsClient.TrackMetric(metricName, value, properties);
        }
    }
}