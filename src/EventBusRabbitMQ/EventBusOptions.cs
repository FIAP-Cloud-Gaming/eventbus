namespace EventBusRabbitMQ;

public class EventBusOptions
{
    /// <summary>
    /// Nome da fila exclusiva deste servico (ex: "payment-service", "identity-service")
    /// </summary>
    public string SubscriptionClientName { get; set; } = string.Empty;

    /// <summary>
    /// Numero de tentativas ao publicar/consumir (default: 10)
    /// </summary>
    public int RetryCount { get; set; } = 10;
}
