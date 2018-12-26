using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Configuration;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions.MongoDB.State;

namespace Orleans.Hosting
{
    public static class MongoDBTransactionsSiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use MongoDB as the default transactional grain storage.
        /// </summary>
        public static ISiloHostBuilder AddMongoDBTransactionalStateStorageAsDefault(this ISiloHostBuilder builder, Action<MongoDBTransactionalStateOptions> configureOptions)
        {
            return builder.AddMongoDBTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use MongoDB for transactional grain storage.
        /// </summary>
        public static ISiloHostBuilder AddMongoDBTransactionalStateStorage(this ISiloHostBuilder builder, string name, Action<MongoDBTransactionalStateOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddMongoDBTransactionalStateStorage(name, ob => ob.Configure(configureOptions)));
        }

        /// <summary>
        /// Configure silo to use MongoDB as the default transactional grain storage.
        /// </summary>
        public static ISiloHostBuilder AddMongoDBTransactionalStateStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<MongoDBTransactionalStateOptions>> configureOptions = null)
        {
            return builder.AddMongoDBTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use MongoDB for transactional grain storage.
        /// </summary>
        public static ISiloHostBuilder AddMongoDBTransactionalStateStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<MongoDBTransactionalStateOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddMongoDBTransactionalStateStorage(name, configureOptions));
        }

        private static IServiceCollection AddMongoDBTransactionalStateStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<MongoDBTransactionalStateOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(Configuration.OptionsServiceCollectionExtensions.AddOptions<MongoDBTransactionalStateOptions>(services, name));

            services.TryAddSingleton<ITransactionalStateStorageFactory>(sp => sp.GetServiceByName<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddSingletonNamedService<ITransactionalStateStorageFactory>(name, MongoDBTransactionalStateStorageFactory.Create);

            return services;
        }
    }
}
