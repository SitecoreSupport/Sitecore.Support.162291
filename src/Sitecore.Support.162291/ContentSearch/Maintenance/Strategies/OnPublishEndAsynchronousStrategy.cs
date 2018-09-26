// --------------------------------------------------------------------------------------------------------------------
// <copyright file="OnPublishEndAsynchronousStrategy.cs" company="Sitecore">
//   Copyright (c) Sitecore. All rights reserved.
// </copyright>
// <summary>
//   Defines the Index Rebuild Strategy on async update
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Threading;
using Sitecore.Abstractions;
using Sitecore.Events;

namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using System.Runtime.Serialization;
  using Sitecore.Common;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Maintenance;
  using Sitecore.Data;
  using Sitecore.Data.Eventing.Remote;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;
  using Sitecore.Eventing.Remote;
  using Sitecore.Globalization;

  /// <summary>
  /// Defines the Index Rebuild Strategy on async update
  /// </summary>
  [DataContract]
  public class OnPublishEndAsynchronousStrategy : BaseAsynchronousStrategy
  {
    private volatile int initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="OnPublishEndAsynchronousStrategy"/> class.
    /// </summary>
    /// <param name="database">
    /// The database.
    /// </param>
    public OnPublishEndAsynchronousStrategy(string database) : base(database)
    {
    }

    /// <summary>
    /// Gets or sets the indexes.
    /// </summary>
    /// <value>
    /// The indexes.
    /// </value>
    protected List<ISearchIndex> Indexes { get; set; }

    /// <summary>
    /// The initialize.
    /// </summary>
    /// <param name="searchIndex">
    /// The index.
    /// </param>
    public override void Initialize(ISearchIndex searchIndex)
    {
      base.Initialize(searchIndex);
      if (this.initialized == 0)
      {
        Interlocked.Increment(ref this.initialized);

        if (Configuration.Settings.EnableEventQueues) // this.Settings
        {
          EventHub.PublishEnd += (sender, args) => this.Handle();
          this.Handle();
        }

        this.Indexes = this.Indexes ?? new List<ISearchIndex>();
      }

      Indexes.Add(searchIndex);
    }

    /// <summary>
    /// The indexing started handler.
    /// </summary>
    /// <param name="sender">The sender.</param>
    /// <param name="args">The args.</param>
    protected override void OnIndexingStarted(object sender, EventArgs args)
    {
      var indexName = Event.ExtractParameter<string>(args, 0);
      var isFullRebuild = (bool)Event.ExtractParameter(args, 1);

      if (this.Indexes.All(x => x.Name != indexName) || !isFullRebuild)
      {
        return;
      }

      QueuedEvent lastEvent = this.Database.RemoteEvents.Queue.GetLastEvent();
      if (lastEvent != null)
      {
        this.IndexTimestamps[indexName] = lastEvent.Timestamp;
      }
    }

    /// <summary>
    /// The indexing ended handler.
    /// </summary>
    /// <param name="sender">The sender.</param>
    /// <param name="args">The args.</param>
    protected override void OnIndexingEnded(object sender, EventArgs args)
    {
      var indexName = Event.ExtractParameter<string>(args, 0);
      var isFullRebuild = (bool)Event.ExtractParameter(args, 1);
      var index = this.Indexes.FirstOrDefault(x => x.Name == indexName);

      if (index != null && isFullRebuild && this.IndexTimestamps.ContainsKey(indexName))
      {
        this.Index.Summary.LastUpdatedTimestamp = this.IndexTimestamps[indexName];
      }
    }

    /// <summary>
    /// The extract uris from queue.
    /// </summary>
    /// <param name="queue">
    /// The queue.
    /// </param>
    /// <returns>
    /// The <see cref="Dictionary{TKey,TValue}"/>.
    /// </returns>
    [Obsolete("ExtractUrisFromQueue(List<QueuedEvent>) method is no longer in use and will be removed in later release.")]
    protected Dictionary<DataUri, DateTime> ExtractUrisFromQueue(List<QueuedEvent> queue)
    {
      var data = new Dictionary<DataUri, DateTime>();

      this.ProcessQueue(
          queue,
          (uri, queuedEvent) =>
          {
            if (!data.Keys.Any(u => u.ItemID == uri.ItemID && string.Equals(u.Language.Name, uri.Language.Name, StringComparison.InvariantCultureIgnoreCase)))
            {
              data.Add(uri, queuedEvent.Created);
            }
          });

      return data;
    }

    /// <summary>
    /// Extracts uris and timestamps from queue.
    /// </summary>
    /// <param name="queue">The event queue.</param>
    /// <returns><see cref="Dictionary{TKey,TValue}"/></returns>
    [Obsolete("ExtractUrisAndTimestampFromQueue(List<QueuedEvent>) method is no longer in use and will be removed in later release.")]
    protected Dictionary<DataUri, long> ExtractUrisAndTimestampFromQueue(List<QueuedEvent> queue)
    {
      var data = new Dictionary<DataUri, long>();

      this.ProcessQueue(queue, (uri, queuedEvent) => data[uri] = queuedEvent.Timestamp);

      return data;
    }

    /// <summary>
    /// Gets the timestamp of the last publishing.
    /// </summary>
    /// <param name="eventQueue">The event queue.</param>
    /// <returns>
    /// The <see cref="long">timestamp</see>.
    /// </returns>
    [Obsolete("GetLastPublishingTimestamp(EventQueue) method is no longer in use and will be removed in later release.")]
    protected virtual long GetLastPublishingTimestamp([NotNull] EventQueue eventQueue)
    {
      Assert.ArgumentNotNull(eventQueue, "eventQueue");
      var query = new EventQueueQuery { EventType = typeof(PublishEndRemoteEvent) };
      List<QueuedEvent> events = eventQueue.GetQueuedEvents(query).ToList();

      if (events.Count > 1)
      {
        return events.OrderByDescending(evt => evt.Timestamp).Skip(1).First().Timestamp;
      }

      return 0L;
    }

    /// <summary>
    /// Process an event queue and invokes action method for each entry.
    /// </summary>
    /// <param name="queue">The event queue.</param>
    /// <param name="addElement">Action method that is invoked for each entry in queue.</param>
    [Obsolete("ProcessQueue(IEnumerable<QueuedEvent>, Action<DataUri, QueuedEvent>) method is no longer in use and will be removed in later release.")]
    private void ProcessQueue(IEnumerable<QueuedEvent> queue, Action<DataUri, QueuedEvent> addElement)
    {
      var serializer = new Serializer();
      foreach (var queuedEvent in queue)
      {
        var instanceData = serializer.Deserialize<SavedItemRemoteEvent>(queuedEvent.InstanceData);

        if (instanceData == null)
        {
          continue;
        }

        var uri = new DataUri(ID.Parse(instanceData.ItemId), Language.Parse(instanceData.LanguageName), Data.Version.Parse(instanceData.VersionNumber));

        addElement(uri, queuedEvent);
      }
    }
  }
}
