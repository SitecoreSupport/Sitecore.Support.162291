namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  using Data.Archiving;
  using Jobs;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Diagnostics;
  using Sitecore.ContentSearch.Maintenance;
  using Sitecore.Data;
  using Sitecore.Data.Eventing.Remote;
  using Sitecore.Eventing;
  using Sitecore.Globalization;
  using System.Collections.Generic;
  using System.Linq;

  public class OnPublishEndAsynchronousStrategy : Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousStrategy
  {
 
    public OnPublishEndAsynchronousStrategy(string database) : base(database)
    {
    }

    protected override void Run(System.Collections.Generic.List<QueuedEvent> queue, ISearchIndex index)
    {
      CrawlingLog.Log.Debug(string.Format("[Index={0}] {1} executing.", index.Name, base.GetType().Name), null);
      if (this.Database == null)
      {
        CrawlingLog.Log.Fatal(string.Format("[Index={0}] OperationMonitor has invalid parameters. Index Update cancelled.", index.Name), null);
        return;
      }
      queue = (from q in queue
               where q.Timestamp > (index.Summary.LastUpdatedTimestamp ?? 0L)
               select q).ToList<QueuedEvent>();
      if (queue.Count <= 0)
      {
        CrawlingLog.Log.Debug(string.Format("[Index={0}] Event Queue is empty. Incremental update returns", index.Name), null);
        return;
      }
      if (!this.CheckForThreshold || queue.Count <= this.ContentSearchSettings.FullRebuildItemCountThreshold())
      {
        System.Collections.Generic.List<IndexableInfo> list = this.ExtractIndexableInfoFromQueue(queue).ToList<IndexableInfo>();
        CrawlingLog.Log.Info(string.Format("[Index={0}] Updating '{1}' items from Event Queue.", index.Name, list.Count<IndexableInfo>()), null);
        Job job = IndexCustodian.IncrementalUpdate(index, list);
        job.Wait();
        return;
      }
      CrawlingLog.Log.Warn(string.Format("[Index={0}] The number of changes exceeded maximum threshold of '{1}'.", index.Name, this.ContentSearchSettings.FullRebuildItemCountThreshold()), null);
      if (this.RaiseRemoteEvents)
      {
        Job job2 = IndexCustodian.FullRebuild(index, true);
        job2.Wait();
        return;
      }
      Job job3 = IndexCustodian.FullRebuildRemote(index, true);
      job3.Wait();
    }

    protected new System.Collections.Generic.IEnumerable<IndexableInfo> ExtractIndexableInfoFromQueue(System.Collections.Generic.List<QueuedEvent> queue)
    {
      System.Collections.Generic.Dictionary<DataUri, IndexableInfo> dictionary = new System.Collections.Generic.Dictionary<DataUri, IndexableInfo>();
      System.Collections.Generic.Dictionary<DataUri, IndexableInfo> dictionary2 = new System.Collections.Generic.Dictionary<DataUri, IndexableInfo>();
      System.Collections.Generic.Dictionary<DataUri, IndexableInfo> dictionary3 = new System.Collections.Generic.Dictionary<DataUri, IndexableInfo>();
      foreach (QueuedEvent current in queue)
      {
        ItemRemoteEventBase itemRemoteEventBase = this.Database.RemoteEvents.Queue.DeserializeEvent(current) as ItemRemoteEventBase;
        if (itemRemoteEventBase != null)
        {
          DataUri dataUri = new DataUri(ID.Parse(itemRemoteEventBase.ItemId), Language.Parse(itemRemoteEventBase.LanguageName), Sitecore.Data.Version.Parse(itemRemoteEventBase.VersionNumber));
          ItemUri itemUri = new ItemUri(dataUri.ItemID, dataUri.Language, dataUri.Version, this.Database);
          IndexableInfo indexable = new IndexableInfo(new SitecoreItemUniqueId(itemUri), current.Timestamp);
          if (itemRemoteEventBase is RemovedVersionRemoteEvent || itemRemoteEventBase is DeletedItemRemoteEvent)
          {
            this.HandleIndexableToRemove(dictionary2, dataUri, indexable);
          }
          else if (itemRemoteEventBase is AddedVersionRemoteEvent)
          {
            this.HandleIndexableToAddVersion(dictionary3, dataUri, indexable);
          }
          else
          {
            this.UpdateIndexableInfo(itemRemoteEventBase, indexable);
            this.HandleIndexableToUpdate(dictionary, dataUri, indexable);
          }
        }
      }
      return (from x in (from x in dictionary
                         select x.Value).Union(from x in dictionary2
                                               select x.Value).Union(from x in dictionary3
                                                                     select x.Value)
              orderby x.Timestamp
              select x).ToList<IndexableInfo>();
    }

    private void HandleIndexableToUpdate(System.Collections.Generic.Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      bool flag = collection.Any((KeyValuePair<DataUri, IndexableInfo> x) => x.Key.ItemID == key.ItemID && x.Value.NeedUpdateChildren);
      bool flag2 = collection.Any((KeyValuePair<DataUri, IndexableInfo> x) => x.Key.ItemID == key.ItemID && x.Value.IsSharedFieldChanged);
      bool flag3 = collection.Any((KeyValuePair<DataUri, IndexableInfo> x) => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language && x.Value.IsUnversionedFieldChanged);
      // Sitecore.Support start
      if (flag)
      {
        IndexableInfo value = collection.FirstOrDefault((KeyValuePair<DataUri, IndexableInfo> x) => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language && x.Key.Version.Number == key.Version.Number).Value;

        if (value != null)
        {
          value.Timestamp = indexable.Timestamp;
          value.NeedUpdateChildren = (value.NeedUpdateChildren || indexable.NeedUpdateChildren);
        }
        else
        {
          (from x in collection.Keys
           where x.ItemID == key.ItemID 
           && x.Language == key.Language 
           select x).ToList<DataUri>().ForEach(delegate (DataUri x)
           {
             collection.Remove(x);
           });

          indexable.NeedUpdateChildren = (flag || indexable.NeedUpdateChildren);
          collection.Add(key, indexable);          
        }

        return;
      }

      if (flag2)
      // Sitecore.Support end
      {
        IndexableInfo value = collection.First((System.Collections.Generic.KeyValuePair<DataUri, IndexableInfo> x) => x.Key.ItemID == key.ItemID).Value;
        value.Timestamp = indexable.Timestamp;
        value.NeedUpdateChildren = (value.NeedUpdateChildren || indexable.NeedUpdateChildren);
        return;
      }
      if (indexable.IsSharedFieldChanged || indexable.NeedUpdateChildren)
      {
        (from x in collection.Keys
         where x.ItemID == key.ItemID
         select x).ToList<DataUri>().ForEach(delegate (DataUri x)
         {
           collection.Remove(x);
         });
        collection.Add(key, indexable);
        return;
      }
      if (flag3)
      {
        collection.First((System.Collections.Generic.KeyValuePair<DataUri, IndexableInfo> x) => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language).Value.Timestamp = indexable.Timestamp;
        return;
      }
      if (indexable.IsUnversionedFieldChanged)
      {
        (from x in collection.Keys
         where x.ItemID == key.ItemID && x.Language == key.Language
         select x).ToList<DataUri>().ForEach(delegate (DataUri x)
         {
           collection.Remove(x);
         });
        collection.Add(key, indexable);
        return;
      }
      if (collection.ContainsKey(key))
      {
        collection[key].Timestamp = indexable.Timestamp;
        return;
      }
      collection.Add(key, indexable);
    }

    private void HandleIndexableToRemove(System.Collections.Generic.Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      if (collection.ContainsKey(key))
      {
        collection[key].Timestamp = indexable.Timestamp;
        return;
      }
      collection.Add(key, indexable);
    }

    private void HandleIndexableToAddVersion(System.Collections.Generic.Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      indexable.IsVersionAdded = true;
      if (!collection.ContainsKey(key))
      {
        collection.Add(key, indexable);
      }
    }

    private void UpdateIndexableInfo(ItemRemoteEventBase instanceData, IndexableInfo indexable)
    {
      if (instanceData is SavedItemRemoteEvent)
      {
        SavedItemRemoteEvent savedItemRemoteEvent = instanceData as SavedItemRemoteEvent;
        if (savedItemRemoteEvent.IsSharedFieldChanged)
        {
          indexable.IsSharedFieldChanged = true;
        }
        if (savedItemRemoteEvent.IsUnversionedFieldChanged)
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
        CopiedItemRemoteEvent copiedItemRemoteEvent = instanceData as CopiedItemRemoteEvent;
        if (copiedItemRemoteEvent.Deep)
        {
          indexable.NeedUpdateChildren = true;
        }
      }
      MovedItemRemoteEvent movedItemRemoteEvent = instanceData as MovedItemRemoteEvent;
      if (movedItemRemoteEvent != null)
      {
        indexable.NeedUpdateChildren = true;
        indexable.OldParentId = movedItemRemoteEvent.OldParentId;
      }
    }
  }
}
