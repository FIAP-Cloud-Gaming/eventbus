namespace EventBusRabbitMQ;

public sealed class RabbitMQEventBus(
    ILogger<RabbitMQEventBus> logger,
    IServiceProvider serviceProvider,
    IConnection connection,
    IOptions<EventBusOptions> options,
    IOptions<EventBusSubscriptionInfo> subscriptionInfo)
    : IEventBus, IHostedService, IDisposable
{
    private const string ExchangeName = "fcg_event_bus";

    private static readonly ActivitySource s_activitySource = new("FiapCloudGaming.EventBus");
    private static readonly TextMapPropagator s_propagator = Propagators.DefaultTextMapPropagator;

    private readonly string _queueName = options.Value.SubscriptionClientName;
    private readonly int _retryCount = options.Value.RetryCount;
    private readonly EventBusSubscriptionInfo _subscriptionInfo = subscriptionInfo.Value;

    private IChannel? _consumerChannel;

    public async Task PublishAsync(IntegrationEvent @event)
    {
        var routingKey = @event.GetType().Name;

        // OpenTelemetry: cria span de publish
        using var activity = s_activitySource.StartActivity($"{routingKey} publish", ActivityKind.Client);

        var properties = new BasicProperties
        {
            DeliveryMode = DeliveryModes.Persistent
        };

        // Propaga trace context nos headers da mensagem
        if (activity is not null)
        {
            s_propagator.Inject(new PropagationContext(activity.Context, Baggage.Current),
                properties, (props, key, value) =>
                {
                    props.Headers ??= new Dictionary<string, object?>();
                    props.Headers[key] = value;
                });
        }

        // Retry com Polly (exponential backoff)
        var policy = Policy.Handle<Exception>()
            .WaitAndRetryAsync(_retryCount,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (ex, time) => logger.LogWarning(ex,
                    "Could not publish {EventName}. Retrying after {Timeout}s...",
                    routingKey, time.TotalSeconds));

        var body = SerializeMessage(@event);

        await policy.ExecuteAsync(async () =>
        {
            await using var channel = await connection.CreateChannelAsync();
            await channel.ExchangeDeclareAsync(ExchangeName, "direct");
            await channel.BasicPublishAsync(ExchangeName, routingKey, true, properties, body);
        });

        logger.LogInformation("Published {EventName} with Id {EventId}", routingKey, @event.Id);
    }

    #region Consumer Setup

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_subscriptionInfo.EventTypes.Count == 0)
        {
            logger.LogInformation("No event subscriptions registered. Skipping consumer setup.");
            return;
        }

        _consumerChannel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        // Declarar exchange + fila + bindings para cada evento registrado
        await _consumerChannel.ExchangeDeclareAsync(ExchangeName, "direct", cancellationToken: cancellationToken);
        await _consumerChannel.QueueDeclareAsync(_queueName, durable: true, exclusive: false,
            autoDelete: false, cancellationToken: cancellationToken);

        foreach (var (eventName, _) in _subscriptionInfo.EventTypes)
        {
            await _consumerChannel.QueueBindAsync(_queueName, ExchangeName, eventName,
                cancellationToken: cancellationToken);
        }

        var consumer = new AsyncEventingBasicConsumer(_consumerChannel);
        consumer.ReceivedAsync += OnMessageReceivedAsync;

        await _consumerChannel.BasicConsumeAsync(_queueName, autoAck: false, consumer: consumer,
            cancellationToken: cancellationToken);

        logger.LogInformation("RabbitMQ consumer started on queue '{Queue}'", _queueName);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumerChannel is not null)
        {
            await _consumerChannel.CloseAsync(cancellationToken);
        }
    }

    #endregion

    #region Processamento de mensagens

    private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs ea)
    {
        var eventName = ea.RoutingKey;

        // OpenTelemetry: extrai trace context dos headers
        var parentContext = s_propagator.Extract(default, ea.BasicProperties,
            (props, key) =>
            {
                if (props.Headers?.TryGetValue(key, out var val) == true && val is byte[] bytes)
                    return [Encoding.UTF8.GetString(bytes)];
                return [];
            });

        using var activity = s_activitySource.StartActivity($"{eventName} process",
            ActivityKind.Server, parentContext.ActivityContext);

        try
        {
            // Resolve handlers via keyed services
            await using var scope = serviceProvider.CreateAsyncScope();
            var handlers = scope.ServiceProvider.GetKeyedServices<IIntegrationEventHandler>(eventName);

            if (!_subscriptionInfo.EventTypes.TryGetValue(eventName, out var eventType))
            {
                logger.LogWarning("No subscription for event '{EventName}'", eventName);
                return;
            }

            var body = Encoding.UTF8.GetString(ea.Body.Span);
            var @event = DeserializeMessage(body, eventType);

            foreach (var handler in handlers)
            {
                await handler.Handle(@event);
            }

            await _consumerChannel!.BasicAckAsync(ea.DeliveryTag, false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing {EventName}", eventName);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

            // Nack sem requeue para ir para dead-letter (se configurado)
            await _consumerChannel!.BasicNackAsync(ea.DeliveryTag, false, false);
        }
    }

    private byte[] SerializeMessage(IntegrationEvent @event)
        => JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), _subscriptionInfo.JsonSerializerOptions);

    private IntegrationEvent DeserializeMessage(string message, Type eventType)
        => (JsonSerializer.Deserialize(message, eventType, _subscriptionInfo.JsonSerializerOptions) as IntegrationEvent)!;

    #endregion

    public void Dispose()
    {
        _consumerChannel?.Dispose();
    }
}
