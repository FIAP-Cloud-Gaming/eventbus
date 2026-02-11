# FIAP Cloud Gaming - EventBus

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Implementacao do padrao Event Bus para comunicacao assincrona entre microsservicos via RabbitMQ. 

## Arquitetura

O EventBus e composto por **dois projetos** seguindo o Dependency Inversion Principle (DIP):

| Projeto | Descricao | Dependencias externas |
|---------|-----------|----------------------|
| `EventBus` | Abstracoes (interfaces + tipos base) | `Microsoft.Extensions.Options` |
| `EventBusRabbitMQ` | Implementacao concreta com RabbitMQ | `RabbitMQ.Client`, `Polly`, `OpenTelemetry.Api` |

Os microsservicos dependem apenas das abstracoes. Para trocar o broker (ex: Azure Service Bus), basta criar um novo projeto de implementacao sem alterar os servicos.

## Funcionalidades

- **Publish/Subscribe** - Publicar integration events e consumir em outros servicos
- **Retry com Polly** - Exponential backoff para resiliencia na publicacao
- **OpenTelemetry** - Propagacao de trace context entre servicos via headers
- **Keyed Services** - Multiplos handlers por evento usando keyed services do .NET 8+
- **Type Registry** - Desserializacao automatica de eventos pelo nome do tipo
- **Hosted Service** - Consumer roda como background service gerenciado pelo host

## Instalacao

Adicionar project references no `.csproj` do microsservico:

```xml
<PackageReference Include="FiapCloudGaming.EventBus" />
<PackageReference Include="FiapCloudGaming.EventBusRabbitMQ" />
```

## Inicio Rapido

### 1. Definir um Integration Event

```csharp
public record PaymentSucceededIntegrationEvent(
    Guid PaymentId,
    int UserId,
    decimal Amount) : IntegrationEvent;
```

### 2. Criar um Handler

```csharp
public class PaymentSucceededIntegrationEventHandler
    : IIntegrationEventHandler<PaymentSucceededIntegrationEvent>
{
    private readonly ILogger<PaymentSucceededIntegrationEventHandler> _logger;

    public PaymentSucceededIntegrationEventHandler(
        ILogger<PaymentSucceededIntegrationEventHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(PaymentSucceededIntegrationEvent @event)
    {
        _logger.LogInformation(
            "Payment {PaymentId} succeeded for user {UserId}. Amount: {Amount}",
            @event.PaymentId, @event.UserId, @event.Amount);

        return Task.CompletedTask;
    }
}
```

### 3. Registrar no DI

```csharp
// Program.cs
builder.AddRabbitMqEventBus(
    builder.Configuration.GetConnectionString("RabbitMQ")!,
    subscriptionClientName: "identity-service")
    .AddSubscription<PaymentSucceededIntegrationEvent, PaymentSucceededIntegrationEventHandler>();
```

### 4. Publicar um Evento

```csharp
public class PaymentSucceededDomainEventHandler(IEventBus eventBus)
    : INotificationHandler<PaymentSucceededDomainEvent>
{
    public async Task Handle(PaymentSucceededDomainEvent notification, CancellationToken ct)
    {
        var integrationEvent = new PaymentSucceededIntegrationEvent(
            notification.PaymentId,
            notification.UserId,
            notification.Amount);

        await eventBus.PublishAsync(integrationEvent);
    }
}
```

## Estrutura dos Projetos

```
EventBus/
  src/
    EventBus/                          # Abstracoes (zero dependencia de broker)
      Abstractions/
        IEventBus.cs                   # interface: PublishAsync(IntegrationEvent)
        IIntegrationEventHandler.cs    # interface: Handle(TEvent)
        IEventBusBuilder.cs            # builder para DI
        EventBusSubscriptionInfo.cs    # registry de tipos de eventos
      Events/
        IntegrationEvent.cs            # record base: Id + CreationDate
      Extensions/
        EventBusBuilderExtensions.cs   # .AddSubscription<TEvent, THandler>()
    EventBusRabbitMQ/                  # Implementacao RabbitMQ
      RabbitMQEventBus.cs              # IEventBus + IHostedService + IDisposable
      EventBusOptions.cs               # SubscriptionClientName + RetryCount
      RabbitMqDependencyInjectionExtensions.cs  # .AddRabbitMqEventBus()
```

## Fluxo de Comunicacao

```
Payment Service                    RabbitMQ                     Identity Service
      |                               |                              |
      | PaymentSucceeded              |                              |
      | DomainEvent                   |                              |
      |----> EventHandler             |                              |
      |      converte para            |                              |
      |      IntegrationEvent         |                              |
      |      IEventBus.PublishAsync ->|                              |
      |      (RabbitMQEventBus)       |                              |
      |      exchange: fcg_event_bus  |                              |
      |      routing: PaymentSucceeded|                              |
      |                               |---> queue: identity-service >|
      |                               |     PaymentSucceeded         |
      |                               |     IntegrationEventHandler  |
```

## Stack

- .NET 10.0
- RabbitMQ.Client 7.x
- Polly 8.x (retry com exponential backoff)
- OpenTelemetry.Api (propagacao de trace context)
- Microsoft.Extensions.Options (configuracao)
- Microsoft.Extensions.Hosting.Abstractions (IHostedService)

## Dependencias

- `Microsoft.Extensions.Options` (10.0.3+)
- `RabbitMQ.Client` (7.2.0+)
- `Polly` (8.6.5+)
- `OpenTelemetry.Api` (1.15.0+)

## Licenca

Este projeto esta licenciado sob a Licenca MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

---

**Feito pelo time FIAP Cloud Gaming**
