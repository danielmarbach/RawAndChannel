using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Raw;
using NServiceBus.Routing;
using NServiceBus.Transport;

namespace RawAndChannel
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var maxConcurrency = 10;

            var channel = Channel.CreateBounded<MessageContext>(new BoundedChannelOptions(maxConcurrency)
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait
            });

            var senderConfig =
                RawEndpointConfiguration.Create("Sender", (ctx, disp) => OnMessage(ctx, disp, channel), "error");
            senderConfig.AutoCreateQueue();
            var transport = senderConfig.UseTransport<RabbitMQTransport>();
            transport.UseConventionalRoutingTopology();
            transport.ConnectionString("host=localhost");
            senderConfig.LimitMessageProcessingConcurrencyTo(maxConcurrency);

            async Task ReadChannel(Channel<MessageContext> channel)
            {
                var contexts = new List<MessageContext>(maxConcurrency);

                await Console.Error.WriteLineAsync("Start reading...");
                while (await channel.Reader.WaitToReadAsync().ConfigureAwait(false))
                {
                    try
                    {
                        await Console.Error.WriteLineAsync("Start TryRead...");
                        while (channel.Reader.TryRead(out var context)) contexts.Add(context);

                        await Console.Error.WriteLineAsync($"Got pushed {contexts.Count} contexts.");
                        foreach (var context in contexts)
                        {
                            await Console.Error.WriteAsync(".");
                            context.Extensions.Get<TaskCompletionSource<bool>>().TrySetResult(true);
                        }
                    }
                    finally
                    {
                        contexts.Clear();
                    }

                    await Console.Error.WriteLineAsync("Done TryRead...");
                }

                await Console.Error.WriteLineAsync("Done reading...");
            }

            var readerTask = Task.Run(() => ReadChannel(channel));

            var sender = await RawEndpoint.Start(senderConfig).ConfigureAwait(false);

            var cts = new CancellationTokenSource();

            async Task ProduceMessages(IReceivingRawEndpoint sender, CancellationToken cancellationToken)
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var transportOperations = new TransportOperation[15];
                    for (var i = 0; i < 15; i++)
                    {
                        var headers = new Dictionary<string, string> {["SomeHeader"] = "SomeValue"};
                        var request = new OutgoingMessage(Guid.NewGuid().ToString(), headers, Array.Empty<byte>());

                        transportOperations[i] = new TransportOperation(request, new UnicastAddressTag("Sender"));
                    }

                    await sender.Dispatch(
                            new TransportOperations(transportOperations), //Can have multiple sends in one batch
                            new TransportTransaction(),
                            new ContextBag())
                        .ConfigureAwait(false);

                    try
                    {
                        await Task.Delay(100, cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        // ignored
                    }
                }

                await Console.Error.WriteLineAsync("Stopped sending");
            }

            var senderTask = Task.Run(() => ProduceMessages(sender, cts.Token), CancellationToken.None);

            await Console.Error.WriteLineAsync("Press any key to stop");
            await Console.In.ReadLineAsync();

            cts.CancelAfter(TimeSpan.FromSeconds(15));
            await sender.StopReceiving();
            channel.Writer.Complete();
            await readerTask;

            await senderTask;
        }

        private static async Task OnMessage(MessageContext context, IDispatchMessages dispatcher,
            Channel<MessageContext> channel)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            context.Extensions.Set(tcs);

            await channel.Writer.WriteAsync(context);

            await tcs.Task;
        }
    }
}