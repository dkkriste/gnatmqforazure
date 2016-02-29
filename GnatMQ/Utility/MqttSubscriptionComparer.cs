namespace GnatMQForAzure.Utility
{
    using System.Collections.Generic;
    using System.Text.RegularExpressions;

    using GnatMQForAzure.Managers;

    /// <summary>
    /// MQTT subscription comparer
    /// </summary>
    public class MqttSubscriptionComparer : IEqualityComparer<MqttSubscription>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="type">MQTT subscription comparer type</param>
        public MqttSubscriptionComparer(MqttSubscriptionComparerType type)
        {
            this.Type = type;
        }

        /// <summary>
        /// MQTT subscription comparer type
        /// </summary>
        public enum MqttSubscriptionComparerType
        {
            OnClientId,
            OnTopic
        }

        /// <summary>
        /// MQTT subscription comparer type
        /// </summary>
        public MqttSubscriptionComparerType Type { get; set; }

        public bool Equals(MqttSubscription x, MqttSubscription y)
        {
            if (this.Type == MqttSubscriptionComparerType.OnClientId)
            {
                return x.ClientId.Equals(y.ClientId);
            }
            else if (this.Type == MqttSubscriptionComparerType.OnTopic)
            {
                return (new Regex(x.Topic)).IsMatch(y.Topic);
            }
            else
            {
                return false;
            }
        }

        public int GetHashCode(MqttSubscription obj)
        {
            return obj.ClientId.GetHashCode();
        }
    }
}