global using System.Text;
global using System.Text.Json;
global using System.Diagnostics;

global using EventBus.Abstractions;
global using EventBus.Events;

global using Microsoft.Extensions.DependencyInjection;
global using Microsoft.Extensions.Hosting;
global using Microsoft.Extensions.Logging;
global using Microsoft.Extensions.Options;

global using OpenTelemetry;
global using OpenTelemetry.Context.Propagation;

global using Polly;

global using RabbitMQ.Client;
global using RabbitMQ.Client.Events;
