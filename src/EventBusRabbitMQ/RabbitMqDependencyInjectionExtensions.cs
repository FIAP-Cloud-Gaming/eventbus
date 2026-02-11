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
        // Registra a conexao RabbitMQ
        builder.Services.AddSingleton<IConnection>(sp =>
        {
            var factory = new ConnectionFactory { Uri = new Uri(connectionString) };
            return factory.CreateConnectionAsync().GetAwaiter().GetResult();
        });

        // Registra opcoes
        builder.Services.Configure<EventBusOptions>(o =>
        {
            o.SubscriptionClientName = subscriptionClientName;
        });

        // Registra o EventBus como IEventBus (para publicar) e IHostedService (para consumir)
        builder.Services.AddSingleton<RabbitMQEventBus>();
        builder.Services.AddSingleton<IEventBus>(sp => sp.GetRequiredService<RabbitMQEventBus>());
        builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<RabbitMQEventBus>());

        return new EventBusBuilder(builder.Services);
    }
}
