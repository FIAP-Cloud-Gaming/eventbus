namespace EventBus.Extensions;

public static class EventBusBuilderExtensions
{
    public static IEventBusBuilder AddSubscription<TEvent, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IEventBusBuilder builder)
            where TEvent : IntegrationEvent
            where THandler : class, IIntegrationEventHandler<TEvent>
    {
        // Registra o handler como keyed service (chave = nome do evento)
        builder.Services.AddKeyedTransient<IIntegrationEventHandler, THandler>(typeof(TEvent));

        // Registra o tipo no mapa de subscriptions para desserializacao
        builder.Services.Configure<EventBusSubscriptionInfo>(o =>
        {
            o.EventTypes[typeof(TEvent).Name] = typeof(TEvent);
        });

        return builder;
    }
}