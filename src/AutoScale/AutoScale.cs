using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent;

namespace AutoScale
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class AutoScale : StatelessService
    {
        public AutoScale(StatelessServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[0];
        }

        private async void CheckCapacity(IAzure azureClient, string scaleSetId, string nodeTypeToScale)
        {
            var scaleSet = azureClient.VirtualMachineScaleSets.GetById(scaleSetId);

            // check custom metrics to determine if capacity needs to increase/decrease
            // .....
            // .....

            

            // simulate capacity needs to increase
            var capacitySettings = new ScaleSetCapacity
            {
                CurrentCapacity = scaleSet.Capacity,
                NewCapacity = scaleSet.Capacity + 1
            };

            // capacity has decreased, so cleanup must take place
            if (capacitySettings.NewCapacity < capacitySettings.CurrentCapacity)
            {
                string mostRecentNodeName = string.Empty;
                using (var client = new FabricClient())
                {
                    var mostRecentLiveNode = (await client.QueryManager.GetNodeListAsync())
                        .Where(n => n.NodeType.Equals(nodeTypeToScale, StringComparison.OrdinalIgnoreCase))
                        .Where(n => n.NodeStatus == System.Fabric.Query.NodeStatus.Up)
                        .OrderByDescending(n => n.NodeInstanceId)
                        .FirstOrDefault();

                    // Remove the node from the Service Fabric cluster
                    ServiceEventSource.Current.ServiceMessage(Context, $"Disabling node {mostRecentLiveNode.NodeName}");
                    mostRecentNodeName = mostRecentLiveNode.NodeName;

                    await client.ClusterManager.DeactivateNodeAsync(mostRecentLiveNode.NodeName, NodeDeactivationIntent.RemoveNode);
                    // Wait (up to a timeout) for the node to gracefully shutdown
                    var timeout = TimeSpan.FromMinutes(5);
                    var waitStart = DateTime.Now;
                    while ((mostRecentLiveNode.NodeStatus == System.Fabric.Query.NodeStatus.Up || mostRecentLiveNode.NodeStatus == System.Fabric.Query.NodeStatus.Disabling) &&
                            DateTime.Now - waitStart < timeout)
                    {
                        mostRecentLiveNode = (await client.QueryManager.GetNodeListAsync()).FirstOrDefault(n => n.NodeName == mostRecentLiveNode.NodeName);
                        await Task.Delay(10 * 1000);
                    }
                }
            }

            // determine if the capacity has changed and update if it has
            if (capacitySettings.NewCapacity != capacitySettings.CurrentCapacity)
            {
                scaleSet.Update().WithCapacity(capacitySettings.NewCapacity).Apply();
            }

        }

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

            long iterations = 0;
            
            /*
                Add the subscription information to the configuration package
            
            
            var configurationPackage = Context.CodePackageActivationContext.GetConfigurationPackageObject("Config");
            var clientId = configurationPackage.Settings.Sections["AutoScale"].Parameters["ClientId"];
            var clientKey = configurationPackage.Settings.Sections["AutoScale"].Parameters["ClientKey"];
            var nodeTypeName = configurationPackage.Settings.Sections["AutoScale"].Parameters["NodeTypeName"];
            var tenantId = configurationPackage.Settings.Sections["AutoScale"].Parameters["TenantId"];
            var subscriptionId = configurationPackage.Settings.Sections["AutoScale"].Parameters["SubscriptionId"];
            */

            string AzureClientId = "[ADD INFO HERE]";
            string AzureClientKey = "[ADD INFO HERE]";
            string AzureTenantId = "[ADD INFO HERE]";
            string AzureSubscriptionId = "[ADD INFO HERE]";
            string ScaleSetId = "AutoScalevmss";
            string NodeTypeToScale = "StatelessFront";

            var credentials = new AzureCredentials(new ServicePrincipalLoginInformation
            {
                ClientId = AzureClientId,
                ClientSecret =
                AzureClientKey
            }, AzureTenantId, AzureEnvironment.AzureGlobalCloud);

            IAzure AzureClient = Azure.Authenticate(credentials).WithSubscription(AzureSubscriptionId);

            if (AzureClient?.SubscriptionId == AzureSubscriptionId)
            {
                ServiceEventSource.Current.ServiceMessage(Context, "Successfully logged into Azure");
            }
            else
            {
                ServiceEventSource.Current.ServiceMessage(Context, "ERROR: Failed to login to Azure");
            }

            while (true)
            {
                CheckCapacity(AzureClient, ScaleSetId, NodeTypeToScale);

                cancellationToken.ThrowIfCancellationRequested();

                ServiceEventSource.Current.ServiceMessage(this.Context, "Working-{0}", ++iterations);

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
