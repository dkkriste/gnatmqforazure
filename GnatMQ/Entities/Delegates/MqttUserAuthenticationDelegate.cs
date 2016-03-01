namespace GnatMQForAzure.Entities.Delegates
{
    /// <summary>
    /// Delegate for executing user authentication
    /// </summary>
    /// <param name="username">Username</param>
    /// <param name="password">Password</param>
    /// <returns></returns>
    public delegate bool MqttUserAuthenticationDelegate(string username, string password);
}