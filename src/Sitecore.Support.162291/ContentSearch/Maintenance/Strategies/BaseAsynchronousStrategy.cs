// --------------------------------------------------------------------------------------------------------------------
// <copyright file="BaseAsynchronousStrategy.cs" company="Sitecore">
//   Copyright (c) Sitecore. All rights reserved.
// </copyright>
// <summary>
//   Defines the Index Rebuild Strategy on async update
// </summary>
// -------------------------------------------------------------------------------------------------------------------- 

namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using System.Globalization;
  using Sitecore.ContentSearch.Diagnostics;
  using Sitecore.ContentSearch.Utilities;
  using Sitecore.Data;
  using Sitecore.Data.Eventing.Remote;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;
  using Sitecore.Globalization;
  using Sitecore.Jobs;
  using System.Threading;
  using Sitecore.Data.Archiving;
  using Sitecore.Abstractions;
  using Sitecore.ContentSearch.Maintenance.Strategies.Models;
  using Sitecore.ContentSearch.Maintenance.Strategies;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Maintenance;

  /// <summary>
  /// Defines the Index Rebuild Strategy on async update
  /// </summary>
  public abstract partial class BaseAsynchronousStrategy : IIndexUpdateStrategy
  {
    /// <summary>
    /// Determines if strategy initialized
    /// </summary>
    private volatile int initialized;

    private ParallelDisabledSecurityProxy parallelDisabledSecurityProxy;

    /// <summary>
    /// Gets the parallel foreach proxy.
    /// </summary>
    /// <value>
    /// The parallel foreach proxy.
    /// </value>
    protected virtual ParallelDisabledSecurityProxy ParallelForeachProxy
    {
        get
        {
            return this.parallelDisabledSecurityProxy = this.parallelDisabledSecurityProxy ?? new ParallelDisabledSecurityProxy();
        }
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="BaseAsynchronousStrategy"/> class.
    /// </summary>
    /// <param name="database">The database.</param>
    protected BaseAsynchronousStrategy(string database)
    {
      Assert.IsNotNullOrEmpty(database, "database");
      this.Database = ContentSearchManager.Locator.GetInstance<IFactory>().GetDatabase(database);
      Assert.IsNotNull(this.Database, string.Format("Database '{0}' was not found", database));
    }

    /// <summary>
    /// Gets or sets the database.
    /// </summary>
    public Database Database { get; protected set; }

    /// <summary>
    /// Gets or sets a value indicating whether check for threshold.
    /// </summary>
    public bool CheckForThreshold { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether strategy should raise remote events.
    /// </summary>
    public bool RaiseRemoteEvents { get; set; }

    /// <summary>
    /// Gets or sets the settings.
    /// </summary>
    internal Sitecore.ContentSearch.Abstractions.ISettings Settings { get; set; }

    /// <summary>
    /// Gets or sets the index.
    /// </summary>
    protected ISearchIndex Index { get; set; }

    /// <summary>
    /// Gets or sets the content search configuration settings.
    /// </summary>
    protected IContentSearchConfigurationSettings ContentSearchSettings { get; set; }

    /// <summary>
    /// Gets or sets the collection of index timestamps.
    /// </summary>
    protected Dictionary<string, long> IndexTimestamps { get; set; }

    /// <summary>The initialize.</summary>
    /// <param name="searchIndex">The index.</param>
    public virtual void Initialize(ISearchIndex searchIndex)
    {
      Assert.IsNotNull(searchIndex, "searchIndex");

      if (Interlocked.CompareExchange(ref this.initialized, 1, 0) == 0)
      {
        this.LogStrategyInitialization(searchIndex);
        this.Index = searchIndex;
        this.Settings = this.Index.Locator.GetInstance<Sitecore.ContentSearch.Abstractions.ISettings>();
        this.ContentSearchSettings = this.Index.Locator.GetInstance<IContentSearchConfigurationSettings>();

        if (!this.Settings.EnableEventQueues())
        {
          CrawlingLog.Log.Fatal(string.Format("[Index={0}] Initialization of {1} failed because event queue is not enabled.", searchIndex.Name, this.GetType().Name));
        }
        else
        {
          if (this.IndexTimestamps == null)
          {
            this.IndexTimestamps = new Dictionary<string, long>();
          }

          EventHub.OnIndexingStarted += this.OnIndexingStarted;
          EventHub.OnIndexingEnded += this.OnIndexingEnded;
        }
      }
    }

    /// <summary>
    /// Runs the pipeline.
    /// </summary>
    public virtual void Run()
    {
      EventManager.RaiseQueuedEvents();

      var eventQueue = this.Database.RemoteEvents.Queue;
      if (eventQueue == null)
      {
        CrawlingLog.Log.Fatal(string.Format("Event Queue is empty. Returning."));
        return;
      }

      var lastUpdatedTimestamp = this.Index.Summary.LastUpdatedTimestamp;
      var queue = this.ReadQueue(eventQueue, lastUpdatedTimestamp);
      var data = this.PrepareIndexData(queue, this.Database);

      this.Run(data, this.Index);
    }

    #region Protected methods

    /// <summary>
    /// Logs initialization information for the strategy.
    /// </summary>
    /// <param name="index">The index which the strategy has been triggered for.</param>
    protected virtual void LogStrategyInitialization(ISearchIndex index)
    {
        var stamp = index.Summary?.LastUpdatedTimestamp;
        CrawlingLog.Log.Info($"[Index={index.Name}] Initializing {this.GetType().Name} [Stamp:{stamp}].");
    }

    /// <summary>
    /// Logs information about an indexable batch which the strategy is processing at the moment.
    /// </summary>
    /// <param name="index">The index which the strategy has been triggered for.</param>
    /// <param name="indexableInfoModel">The indexable batch proccessed by the strategy.</param>
    /// <exception cref="T:System.ArgumentNullException"><paramref name="index" /> is null.</exception>
    /// <exception cref="T:System.ArgumentNullException"><paramref name="indexableInfoModel" /> is null.</exception>
    protected virtual void LogUpdateBatchInfo([NotNull]ISearchIndex index, [NotNull]List<IndexableInfoModel> indexableInfoModel)
    {
        Debug.ArgumentNotNull(index, "index");
        Debug.ArgumentNotNull(indexableInfoModel, "indexableInfoModel");

        long? lowerBoundary = null;
        long? upperBoundary = null;
        if (indexableInfoModel.Count > 0)
        {
            lowerBoundary = indexableInfoModel[0]?.TimeStamp;
            upperBoundary = indexableInfoModel[indexableInfoModel.Count - 1]?.TimeStamp;
        }

        CrawlingLog.Log.Debug(string.Format(CultureInfo.InvariantCulture, "[Index={0}] {1}: Batch for processing [{2}; {3}] Count: {4}", 
            index.Name, this.GetType().Name, lowerBoundary, upperBoundary, indexableInfoModel.Count));
    }

    protected virtual List<IndexableInfoModel> PrepareIndexData(List<QueuedEvent> queue, Database db)
    {
      var data = new List<IndexableInfoModel>(queue.Count);

      using (var deserializer = new CacheableDeserializer(this.Database.RemoteEvents.Queue))
      {
        foreach (var queuedEvent in queue)
        {
          var instanceData = deserializer.DeserializeEvent(queuedEvent) as ItemRemoteEventBase;

          if (instanceData != null)
          {
            var dataUri = new DataUri(ID.Parse(instanceData.ItemId), Language.Parse(instanceData.LanguageName), Data.Version.Parse(instanceData.VersionNumber));
            var itemUri = new ItemUri(dataUri.ItemID, dataUri.Language, dataUri.Version, db);
            data.Add(new IndexableInfoModel (dataUri, new SitecoreItemUniqueId(itemUri), instanceData, queuedEvent.Timestamp));
          }
        }
      }

      return data;
    }

    /// <summary>
    /// The handle.
    /// </summary>
    protected void Handle()
    {
      OperationMonitor.Register(this.Run, this.Index.Name);
      OperationMonitor.Trigger();
    }

    /// <summary>
    /// The read queue.
    /// </summary>
    /// <param name="eventQueue">The event queue.</param>
    /// <returns>
    /// The <see cref="List{QueuedEvent}" />.
    /// </returns>
    protected virtual List<QueuedEvent> ReadQueue(EventQueue eventQueue)
    {
      return this.ReadQueue(eventQueue, this.Index.Summary.LastUpdatedTimestamp);
    }

    /// <summary>
    /// Reads the queue.
    /// </summary>
    /// <param name="eventQueue">The event queue.</param>
    /// <param name="lastUpdatedTimestamp">The last updated timestamp.</param>
    /// <returns>
    /// The list of queued events.
    /// </returns>
    protected virtual List<QueuedEvent> ReadQueue(EventQueue eventQueue, long? lastUpdatedTimestamp)
    {
      var queue = new List<QueuedEvent>();

      lastUpdatedTimestamp = lastUpdatedTimestamp ?? 0;

      var query = new EventQueueQuery { FromTimestamp = lastUpdatedTimestamp };
      query.EventTypes.Add(typeof(RemovedVersionRemoteEvent));
      query.EventTypes.Add(typeof(SavedItemRemoteEvent));
      query.EventTypes.Add(typeof(DeletedItemRemoteEvent));
      query.EventTypes.Add(typeof(MovedItemRemoteEvent));
      query.EventTypes.Add(typeof(AddedVersionRemoteEvent));
      query.EventTypes.Add(typeof(CopiedItemRemoteEvent));
      query.EventTypes.Add(typeof(RestoreItemCompletedEvent));

      queue.AddRange(eventQueue.GetQueuedEvents(query));

      return queue.Where(e => e.Timestamp > lastUpdatedTimestamp).ToList();
    }

    /// <summary>
    /// Extracts instances of <see cref="IndexableInfo"/> from queue.
    /// </summary>
    /// <param name="queue">The event queue.</param>
    /// <returns><see cref="IEnumerable{T}"/></returns>

    [Obsolete("ExtractIndexableInfoFromQueue method is obsolete. Please use PrepareIndexableInfo instead.")]
    protected IEnumerable<IndexableInfo> ExtractIndexableInfoFromQueue(List<QueuedEvent> queue)
    {
      var indexableListToUpdate = new DataUriBucketDictionary<IndexableInfo>();
      var indexableListToRemove = new DataUriBucketDictionary<IndexableInfo>();
      var indexableListToAddVersion = new DataUriBucketDictionary<IndexableInfo>();

      foreach (var queuedEvent in queue)
      {
        var instanceData = this.Database.RemoteEvents.Queue.DeserializeEvent(queuedEvent) as ItemRemoteEventBase;

        if (instanceData == null)
        {
          continue;
        }

        var key = new DataUri(ID.Parse(instanceData.ItemId), Language.Parse(instanceData.LanguageName), Data.Version.Parse(instanceData.VersionNumber));
        var itemUri = new ItemUri(key.ItemID, key.Language, key.Version, this.Database);
        var indexable = new IndexableInfo(new SitecoreItemUniqueId(itemUri), queuedEvent.Timestamp);

        if (instanceData is RemovedVersionRemoteEvent || instanceData is DeletedItemRemoteEvent)
        {
          this.HandleIndexableToRemove(indexableListToRemove, key, indexable);
        }
        else if (instanceData is AddedVersionRemoteEvent)
        {
          this.HandleIndexableToAddVersion(indexableListToAddVersion, key, indexable);
        }
        else
        {
          this.UpdateIndexableInfo(instanceData, indexable);
          this.HandleIndexableToUpdate(indexableListToUpdate, key, indexable);
        }
      }

      return indexableListToUpdate.ExtractValues()
        .Concat(indexableListToRemove.ExtractValues())
        .Concat(indexableListToAddVersion.ExtractValues())
        .OrderBy(x => x.Timestamp).ToList();
    }

    protected virtual IndexableInfo[] PrepareIndexableInfo(List<IndexableInfoModel> data, long lastUpdatedStamp)
    {
      var indexableListToUpdate = new DataUriBucketDictionary<IndexableInfo>();
      var indexableListToRemove = new DataUriBucketDictionary<IndexableInfo>();
      var indexableListToAddVersion = new DataUriBucketDictionary<IndexableInfo>();

      foreach (var instData in data)
      {
        if (instData.TimeStamp <= lastUpdatedStamp)
        {
          // This record has already been proccessed, skip it
          continue;
        }

        var key = instData.Key;
        var indexable = new IndexableInfo(instData.UniqueId, instData.TimeStamp);

        if (instData.RemoteEvent is RemovedVersionRemoteEvent || instData.RemoteEvent is DeletedItemRemoteEvent)
        {
          this.HandleIndexableToRemove(indexableListToRemove, key, indexable);
        }
        else if (instData.RemoteEvent is AddedVersionRemoteEvent)
        {
          this.HandleIndexableToAddVersion(indexableListToAddVersion, key, indexable);
        }
        else
        {
          this.UpdateIndexableInfo(instData.RemoteEvent, indexable);
          this.HandleIndexableToUpdate(indexableListToUpdate, key, indexable);
        }
      }

      return
        indexableListToUpdate.ExtractValues()
          .Concat(indexableListToRemove.ExtractValues())
          .Concat(indexableListToAddVersion.ExtractValues())
          .OrderBy(x => x.Timestamp)
          .ToArray();
    }

    /// <summary>
    /// Runs the specified queue.
    /// </summary>
    /// <param name="data">The queue.</param>
    /// <param name="index">The index.</param>
    [Obsolete("Use another overload method of Run instead.")]
    protected virtual void Run(List<QueuedEvent> data, ISearchIndex index)
    {
      List<IndexableInfoModel> transformedList = this.PrepareIndexData(data, this.Database);
      this.Run(transformedList, index);
    }

    /// <summary>
    /// Runs the specified queue.
    /// </summary>
    /// <param name="data">The queue.</param>
    /// <param name="index">The index.</param>
    protected virtual void Run(List<IndexableInfoModel> data, ISearchIndex index)
    {
      CrawlingLog.Log.Debug(string.Format("[Index={0}] {1} executing.", index.Name, this.GetType().Name));

      if (this.Database == null)
      {
        CrawlingLog.Log.Fatal(
          string.Format("[Index={0}] OperationMonitor has invalid parameters. Index Update cancelled.", index.Name),
          null);
        return;
      }

      var lastUpdatedStamp = index.Summary.LastUpdatedTimestamp ?? 0L;
      int elementsToIndexCount = data.Count(q => q.TimeStamp > lastUpdatedStamp);

      if (elementsToIndexCount <= 0)
      {
        CrawlingLog.Log.Debug(
          string.Format("[Index={0}] Event Queue is empty. Incremental update returns", index.Name), null);
        return;
      }

      if (!this.CheckForThreshold || elementsToIndexCount <= this.ContentSearchSettings.FullRebuildItemCountThreshold())
      {
        this.LogUpdateBatchInfo(index, data);
        var indexableInfos = this.PrepareIndexableInfo(data, lastUpdatedStamp);
        var incrementalUpdatedJob = IndexCustodian.IncrementalUpdate(index, indexableInfos);
        incrementalUpdatedJob.Wait();
        return;
      }

      if (this.RaiseRemoteEvents)
      {
        var fullRebuildJob = IndexCustodian.FullRebuild(index, true);
        fullRebuildJob.Wait();
        return;
      }

      var fullRebuildRemoteJob = IndexCustodian.FullRebuildRemote(index, true);
      fullRebuildRemoteJob.Wait();
    }

    /// <summary>
    /// The indexing started handler.
    /// </summary>
    /// <param name="sender">The sender.</param>
    /// <param name="args">The args.</param>
    protected virtual void OnIndexingStarted(object sender, EventArgs args)
    {
      var indexName = ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter<string>(args, 0);
      var isFullRebuild = (bool)ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter(args, 1);

      if (this.Index.Name == indexName && isFullRebuild)
      {
        QueuedEvent lastEvent = this.Database.RemoteEvents.Queue.GetLastEvent();
        this.IndexTimestamps[indexName] = lastEvent == null ? 0 : lastEvent.Timestamp;
      }
    }

    /// <summary>
    /// The indexing ended handler.
    /// </summary>
    /// <param name="sender">The sender.</param>
    /// <param name="args">The args.</param>
    protected virtual void OnIndexingEnded(object sender, EventArgs args)
    {
      var indexName = ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter<string>(args, 0);
      var isFullRebuild = (bool)ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter(args, 1);

      if (this.Index.Name == indexName && isFullRebuild && this.IndexTimestamps.ContainsKey(indexName))
      {
        this.Index.Summary.LastUpdatedTimestamp = this.IndexTimestamps[indexName];
      }
    }
    
    #endregion

    #region Private methods

    private void HandleIndexableToRemove(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      if (collection.ContainsKey(key))
      {
        collection[key].Timestamp = indexable.Timestamp;
      }
      else
      {
        collection.Add(key, indexable);
      }
    }

    private void HandleIndexableToAddVersion(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      indexable.IsVersionAdded = true;
      if (!collection.ContainsKey(key))
      {
        collection.Add(key, indexable);
      }
    }

    private bool AlreadyAddedMovedItemEvent(DataUriBucketDictionary<IndexableInfo> collection, ID id)
    {
      return collection.ContainsAny(id, x => x.Value.NeedUpdateChildren);
    }

    private bool AlreadyAddedSharedFieldChange(DataUriBucketDictionary<IndexableInfo> collection, ID id)
    {
      return collection.ContainsAny(id, x => x.Value.IsSharedFieldChanged);
    }

    private bool AlreadyAddedUnversionedFieldChange(DataUriBucketDictionary<IndexableInfo> collection, DataUri key)
    {
      return collection.ContainsAny(key.ItemID, x => x.Key.Language == key.Language && x.Value.IsUnversionedFieldChanged);
    }

    private void HandleIndexableToUpdate(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      bool alreadySetNeedUpdateChildren = collection.ContainsAny(key.ItemID, x => x.Value.NeedUpdateChildren);
      bool alreadyAddedSharedFieldChange = collection.ContainsAny(key.ItemID, x => x.Value.IsSharedFieldChanged);
      bool alreadyAddedUnversionedFieldChange = collection.ContainsAny(key.ItemID, x => x.Key.Language == key.Language && x.Value.IsUnversionedFieldChanged);

      #region FIX 162291
      if (alreadySetNeedUpdateChildren)
      {
        IndexableInfo value = null;
        try
        {
          value = collection.First(key.ItemID, (KeyValuePair<DataUri, IndexableInfo> x) => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language && x.Key.Version.Number == key.Version.Number);
        }
        catch
        {
          value = null;
        }

        if (value != null)
        {
          value.Timestamp = indexable.Timestamp;
          value.NeedUpdateChildren = value.NeedUpdateChildren || indexable.NeedUpdateChildren;
        }
        else
        {
          collection.RemoveAll(key.ItemID, (x) => x.ItemID == key.ItemID && x.Language == key.Language);
          indexable.NeedUpdateChildren = (alreadySetNeedUpdateChildren || indexable.NeedUpdateChildren);
          collection.Add(key, indexable);
        }
      }
      else if (alreadyAddedSharedFieldChange)
      #endregion
      {
        // collection.Keys.Where(x => x.ItemID == key.ItemID && x.Language == key.Language).ToList().ForEach(x => collection.Remove(x));
        collection.RemoveAll(key.ItemID, x => x.Language == key.Language);
        collection.Add(key, indexable);
      }
      else
      {
        if (collection.ContainsKey(key))
        {
          collection[key].Timestamp = indexable.Timestamp;
        }
        else
        {
          collection.Add(key, indexable);
        }
      }
    }

    private void UpdateIndexableInfo(ItemRemoteEventBase instanceData, IndexableInfo indexable)
    {
      if (instanceData is SavedItemRemoteEvent)
      {
        var savedEvent = instanceData as SavedItemRemoteEvent;

        if (savedEvent.IsSharedFieldChanged)
        {
          indexable.IsSharedFieldChanged = true;
        }

        if (savedEvent.IsUnversionedFieldChanged)
        {
          indexable.IsUnversionedFieldChanged = true;
        }
      }

      if (instanceData is RestoreItemCompletedEvent)
      {
        indexable.IsSharedFieldChanged = true;
      }

      if (instanceData is CopiedItemRemoteEvent)
      {
        indexable.IsSharedFieldChanged = true;
        var copiedItemData = instanceData as CopiedItemRemoteEvent;
        if (copiedItemData.Deep)
        {
          indexable.NeedUpdateChildren = true;
        }
      }

      var @event = instanceData as MovedItemRemoteEvent;
      if (@event != null)
      {
        indexable.NeedUpdateChildren = true;
        indexable.OldParentId = @event.OldParentId;
      }
    }

    #endregion
  }
}
