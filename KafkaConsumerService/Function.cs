using Amazon.Lambda.Core;
using KafkaConsumerService.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace KafkaConsumerService;

public class Function
{
    readonly ServiceProvider _serviceProvider;

    public Function()
    {
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton<IHostedService, ConsumerService>();
        _serviceProvider = serviceCollection.BuildServiceProvider();
    }

    /// <summary>
    /// A simple function that takes a string and does a ToUpper
    /// </summary>
    /// <param name="input"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public async Task<string> FunctionHandler(string input, ILambdaContext context)
    {
        using (var scope = _serviceProvider.CreateScope())
        {
            var cancelation = new CancellationToken();
            var service = scope.ServiceProvider.GetRequiredService<ConsumerService>();

            try
            {
                await service.StartAsync(cancelation);
                return "success";
            }
            catch (Exception ex)
            {
                await service.StopAsync(cancelation);
            }
        }
        return "fail";
    }
}
