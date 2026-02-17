namespace EventBusRabbitMQ;

public static class RabbitMqDependencyInjectionExtensions
{
    private class EventBusBuilder(IServiceCollection services) : IEventBusBuilder
    {
        public IServiceCollection Services => services;
    }

    public static IEventBusBuilder AddRabbitMqEventBus(
        this IHostApplicationBuilder builder,
        string connectionString,
        string subscriptionClientName)
    {
        builder.Services.AddSingleton<IConnection>(sp =>
        {
            var factory = new ConnectionFactory { Uri = new Uri(connectionString) };
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("RabbitMQ");

            const int maxRetries = 10;
            const int baseDelaySeconds = 2;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    logger.LogInformation("Connecting to RabbitMQ (attempt {Attempt}/{Max})...", attempt, maxRetries);
                    var connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
                    logger.LogInformation("Successfully connected to RabbitMQ");
                    return connection;
                }
                catch (Exception ex) when (attempt < maxRetries)
                {
                    var delay = TimeSpan.FromSeconds(baseDelaySeconds * attempt);
                    logger.LogWarning(ex, "RabbitMQ not ready. Retrying in {Delay}s (attempt {Attempt}/{Max})...",
                        delay.TotalSeconds, attempt, maxRetries);
                    Thread.Sleep(delay);
                }
            }

            return factory.CreateConnectionAsync().GetAwaiter().GetResult();
        });

        builder.Services.Configure<EventBusOptions>(o =>
        {
            o.SubscriptionClientName = subscriptionClientName;
        });

        builder.Services.AddSingleton<RabbitMQEventBus>();
        builder.Services.AddSingleton<IEventBus>(sp => sp.GetRequiredService<RabbitMQEventBus>());
        builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<RabbitMQEventBus>());

        return new EventBusBuilder(builder.Services);
    }
}
