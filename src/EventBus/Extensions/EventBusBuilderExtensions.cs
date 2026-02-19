namespace EventBus.Extensions;

public static class EventBusBuilderExtensions
{
    public static IEventBusBuilder AddSubscription<TEvent, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IEventBusBuilder builder)
            where TEvent : IntegrationEvent
            where THandler : class, IIntegrationEventHandler<TEvent>
    {
        builder.Services.AddKeyedTransient<IIntegrationEventHandler, THandler>(typeof(TEvent).Name);

        builder.Services.Configure<EventBusSubscriptionInfo>(o =>
        {
            o.EventTypes[typeof(TEvent).Name] = typeof(TEvent);
        });

        return builder;
    }
}