using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using static Newtonsoft.Json.JsonConvert;

namespace Orleans.Transactions.MongoDB.State
{
    public class MongoDBTransactionalStateStorage<TState> : ITransactionalStateStorage<TState>
        where TState : class, new()
    {
        private static readonly UpdateOptions Upsert = new UpdateOptions { IsUpsert = true };
        private static readonly FilterDefinitionBuilder<BsonDocument> Filter = Builders<BsonDocument>.Filter;
        private static readonly UpdateDefinitionBuilder<BsonDocument> Update = Builders<BsonDocument>.Update;

        private const string FieldId = "_id";
        private const string FieldEtag = "_etag";

        private readonly ILogger _logger;
        private readonly IGrainActivationContext _context;
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly Newtonsoft.Json.JsonSerializerSettings jsonSettings;

        private TransactionalStateRecord<TState> State { get; set; }

        public MongoDBTransactionalStateStorage(
            IMongoCollection<BsonDocument> collection,
            Newtonsoft.Json.JsonSerializerSettings jsonSettings,
            ILogger<MongoDBTransactionalStateStorage<TState>> logger,
            IGrainActivationContext context)
        {
            this._logger = logger;
            this._context = context;
            this._collection = collection;
            this.jsonSettings = jsonSettings;
        }

        public async Task<TransactionalStorageLoadResponse<TState>> Load()
        {
            await this.ReadRecordAsync().ConfigureAwait(false);
            if (string.IsNullOrEmpty(this.State.ETag))
            {
                if (this._logger.IsEnabled(LogLevel.Debug))
                    this._logger.LogDebug($"{this.State.Id} Loaded v0, fresh");

                // first time load
                return new TransactionalStorageLoadResponse<TState> { };
            }

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug($"{this.State.Id} Loaded v{this.State.CommittedSequenceId}");

            return new TransactionalStorageLoadResponse<TState>(this.State.ETag,
                this.State.CommittedState,
                this.State.CommittedSequenceId,
                this.State.Metadata,
                this.State.PendingStates);
        }

        public async Task<string> Store(string expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<TState>> statesToPrepare, long? commitUpTo, long? abortAfter)
        {
            if (this.State.ETag != expectedETag)
            {
                throw new ArgumentException(nameof(expectedETag), "Etag does not match");
            }

            var pendinglist = this.State.PendingStates;

            // abort
            if (abortAfter.HasValue && pendinglist.Count != 0)
            {
                var pos = pendinglist.FindIndex(t => t.SequenceId > abortAfter.Value);
                if (pos != -1)
                {
                    pendinglist.RemoveRange(pos, pendinglist.Count - pos);
                }
            }

            // prepare
            if (statesToPrepare?.Count > 0)
            {
                foreach (var p in statesToPrepare)
                {
                    var pos = pendinglist.FindIndex(t => t.SequenceId >= p.SequenceId);
                    if (pos == -1)
                    {
                        pendinglist.Add(p); //append
                    }
                    else if (pendinglist[pos].SequenceId == p.SequenceId)
                    {
                        pendinglist[pos] = p;  //replace
                    }
                    else
                    {
                        pendinglist.Insert(pos, p); //insert
                    }
                }
            }

            // commit
            if (commitUpTo.HasValue && commitUpTo.Value > this.State.CommittedSequenceId)
            {
                var pos = pendinglist.FindIndex(t => t.SequenceId == commitUpTo.Value);
                if (pos != -1)
                {
                    var committedState = pendinglist[pos];
                    this.State.CommittedSequenceId = committedState.SequenceId;
                    this.State.CommittedState = committedState.State;
                    pendinglist.RemoveRange(0, pos + 1);
                }
                else
                {
                    throw new InvalidOperationException($"Transactional state corrupted. Missing prepare record (SequenceId={commitUpTo.Value}) for committed transaction.");
                }
            }

            await this.WriteRecordAsync().ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug($"{this.State.Id} Stored v{this.State.CommittedSequenceId} eTag={this.State.ETag}");

            return this.State.ETag;
        }

        public async Task ReadRecordAsync()
        {
            var grainKey = this._context.GrainInstance.GrainReference.ToShortKeyString();
            var filter = new BsonDocument(FieldId, grainKey);
            var doc = await this._collection.Find(filter)
                        .FirstOrDefaultAsync().ConfigureAwait(false);
            if (doc == null)
                this.State = new TransactionalStateRecord<TState> { Id = grainKey };
            else
            {
                this.State = new TransactionalStateRecord<TState>
                {
                    Id = doc[FieldId].AsString,
                    ETag = doc[FieldEtag].AsString,
                    CommittedState = DeserializeObject<TState>(doc[nameof(State.CommittedState)].AsString, this.jsonSettings),
                    CommittedSequenceId = doc[nameof(State.CommittedSequenceId)].AsInt64,
                    Metadata = DeserializeObject<TransactionalStateMetaData>(doc[nameof(State.Metadata)].AsString, this.jsonSettings),
                    PendingStates = DeserializeObject<List<PendingTransactionState<TState>>>(doc[nameof(State.PendingStates)].AsString, this.jsonSettings)
                };
            }
        }

        public async Task WriteRecordAsync()
        {
            var grainKey = this._context.GrainInstance.GrainReference.ToShortKeyString();
            var eTag = this.State.ETag;
            this.State.ETag = Guid.NewGuid().ToString();

            var ret = await this._collection.UpdateOneAsync(
                Filter.And(
                    Filter.Eq(FieldId, grainKey),
                    Filter.Eq(FieldEtag, eTag)
                ), Update
                    .Set(FieldEtag, this.State.ETag)
                    .Set(nameof(this.State.CommittedState), SerializeObject(this.State.CommittedState, this.jsonSettings))
                    .Set(nameof(this.State.CommittedSequenceId), this.State.CommittedSequenceId)
                    .Set(nameof(this.State.Metadata), SerializeObject(this.State.Metadata, this.jsonSettings))
                    .Set(nameof(this.State.PendingStates), SerializeObject(this.State.PendingStates, this.jsonSettings))
                , Upsert).ConfigureAwait(false);
        }

    }

    [Serializable]
    public class TransactionalStateRecord<TState> where TState : class, new()
    {
        public string Id;
        public string ETag { get; set; }

        public TState CommittedState { get; set; } = new TState();

        public long CommittedSequenceId { get; set; }

        public TransactionalStateMetaData Metadata { get; set; } = new TransactionalStateMetaData();

        public List<PendingTransactionState<TState>> PendingStates { get; set; } = new List<PendingTransactionState<TState>>();

    }
}
