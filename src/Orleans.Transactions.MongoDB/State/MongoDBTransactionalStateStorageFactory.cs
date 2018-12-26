using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.MongoDB.State
{
    public class MongoDBTransactionalStateStorageFactory : ITransactionalStateStorageFactory
    {
        private readonly string name;
        private readonly ClusterOptions clusterOptions;
        private readonly ILoggerFactory loggerFactory;
        private readonly IMongoDatabase db;
        private readonly Newtonsoft.Json.JsonSerializerSettings jsonSettings;

        public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
        {
            var optionsSnapshot =
                services.GetRequiredService<IOptionsSnapshot<MongoDBTransactionalStateOptions>>();

            var op = optionsSnapshot.Get(name);
            return ActivatorUtilities.CreateInstance<MongoDBTransactionalStateStorageFactory>(services, name, op);
        }

        public MongoDBTransactionalStateStorageFactory(string name,
            MongoDBTransactionalStateOptions options,
            IOptions<ClusterOptions> clusterOptions,
            ITypeResolver typeResolver,
            IGrainFactory grainFactory,
            ILoggerFactory loggerFactory)
        {
            this.name = name;
            this.jsonSettings = TransactionalStateFactory.GetJsonSerializerSettings(
                typeResolver,
                grainFactory);
            var client = new MongoClient(options.ConnectionString);
            db = client.GetDatabase(options.Database);
            this.clusterOptions = clusterOptions.Value;
            this.loggerFactory = loggerFactory;
        }

        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainActivationContext context) where TState : class, new()
        {
            return ActivatorUtilities.CreateInstance<MongoDBTransactionalStateStorage<TState>>(
                context.ActivationServices,
                this.db.GetCollection<BsonDocument>("tsr_" + stateName), jsonSettings);
        }
    }
}
