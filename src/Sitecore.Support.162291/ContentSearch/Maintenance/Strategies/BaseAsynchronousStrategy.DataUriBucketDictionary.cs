namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using Data;
  using Sitecore.Diagnostics;

  public abstract partial class BaseAsynchronousStrategy
  {
    /// <summary>
    /// Represents a collection of DataUris and values stored in buckets based on the ID of the Item that DataUri refer to.
    /// </summary>
    /// <typeparam name="TValue">The type of values in the dictionary</typeparam>
    protected internal class DataUriBucketDictionary<TValue>
    {
      #region Private members

      private Dictionary<ID, Dictionary<DataUri, TValue>> storage = new Dictionary<ID, Dictionary<DataUri, TValue>>();

      private Dictionary<DataUri, TValue> FindBucket(ID id)
      {
        Assert.ArgumentNotNull(id, "id");

        Dictionary<DataUri, TValue> bucket;
        if (!storage.TryGetValue(id, out bucket))
        {
          return null;
        }

        return bucket;
      }

      private Dictionary<DataUri, TValue> FindBucket(DataUri key)
      {
        Assert.ArgumentNotNull(key, "key");

        return this.FindBucket(key.ItemID);
      }

      private void DeleteBucket(ID bucketId)
      {
        Assert.ArgumentNotNull(bucketId, "bucketId");

        storage.Remove(bucketId);
      }

      #endregion

      #region Public members

      /// <summary>
      /// Gets or sets the value associated with the specified DataUri key inside the bucket
      /// </summary>
      /// <param name="uri">The key of the value to get or set.</param>
      /// <returns>
      /// The value associated with the specified key. If the specified key is not found 
      /// a set operation creates a new element with the specified key.
      /// </returns>
      public TValue this[DataUri uri]
      {
        get
        {
          Assert.ArgumentNotNull(uri, "uri");

          return storage[uri.ItemID][uri];
        }
        set
        {
          var bucket = this.FindBucket(uri);
          if (bucket == null)
          {
            this.Add(uri, value);
          }
          else
          {
            bucket[uri] = value;
          }
        }
      }

      /// <summary>
      /// Adds the specified key and value to the dictionary. 
      /// </summary>
      /// <param name="key">The key of the element to add.</param>
      /// <param name="value">The value of the element to add. The value can be null for reference types.</param>
      public void Add(DataUri key, TValue value)
      {
        Assert.ArgumentNotNull(key, "key");

        var bucket = FindBucket(key);

        if (bucket == null)
        {
          bucket = new Dictionary<DataUri, TValue>();
          storage.Add(key.ItemID, bucket);
        }

        bucket.Add(key, value);
      }

      /// <summary>
      /// Determines whether the collections contains the specified key.
      /// </summary>
      /// <param name="key"> The key to locate in the collections.</param>
      /// <returns>true if the collection contains an element with the specified key; otherwise, false.</returns>
      public bool ContainsKey(DataUri key)
      {
        Assert.ArgumentNotNull(key, "key");

        var bucket = FindBucket(key);

        if (bucket == null)
        {
          return false;
        }

        return bucket.ContainsKey(key);
      }

      /// <summary>
      /// Determines whether any element of the collection satisfies the condition.
      /// </summary>
      /// <param name="bucketId">The bucket id to search in.</param>
      /// <param name="predicate">A function to test each element for a condition.</param>
      /// <returns></returns>
      public bool ContainsAny(ID bucketId, Func<KeyValuePair<DataUri, TValue>, bool> predicate = null)
      {
        Assert.ArgumentNotNull(bucketId, "bucketId");

        var bucket = FindBucket(bucketId);
        if (bucket == null)
        {
          return false;
        }

        if (predicate == null)
        {
          return bucket.Count > 0;
        }

        return bucket.Any(predicate);
      }

      /// <summary>
      /// Removes all the elements of the collection satisfies the condition under the given bucket id.
      /// </summary>
      /// <param name="bucketId">Bucket id</param>
      /// <param name="predicate">A function to test each element for a condition.</param>
      public void RemoveAll(ID bucketId, Func<DataUri, bool> predicate = null)
      {
        Assert.ArgumentNotNull(bucketId, "bucketId");

        var bucket = FindBucket(bucketId);
        if (bucket == null)
        {
          return;
        }

        if (predicate != null)
        {
          var keys = bucket.Keys.Where(predicate).ToList();
          if (keys.Count == 0)
          {
            return;
          }

          keys.ForEach(x => bucket.Remove(x));
        }

        if (predicate == null || bucket.Count == 0)
        {
          DeleteBucket(bucketId);
        }
      }

      /// <summary>
      /// Returns the first element of the collection that satisfies a specified condition under the given bucket id.
      /// </summary>
      /// <param name="bucketId">Bucket id</param>
      /// <param name="predicate"></param>
      /// <returns>A function to test each element for a condition.</returns>
      public TValue First(ID bucketId, Func<KeyValuePair<DataUri, TValue>, bool> predicate = null)
      {
        var bucket = FindBucket(bucketId);

        Assert.IsNotNull(bucket, "Can't find a record which satisfies the condition...");

        if (predicate == null)
        {
          return bucket.First().Value;
        }

        return bucket.First(predicate).Value;
      }

      /// <summary>
      /// Projects each value of each bucket into an <see cref="IEnumerable{TValue}"/>.
      /// </summary>
      /// <returns>An <see cref="IEnumerable{TValue}"/> of all the values from all the buckets</returns>
      public IEnumerable<TValue> ExtractValues()
      {
        return storage.Values.SelectMany(values => values.Values, (values, value) => value);
      }

      /// <summary>
      /// Get the value from dictionary
      /// </summary>
      /// <param name="key"></param>
      /// <param name="value"></param>
      /// <returns></returns>
      public bool TryGetValue(DataUri key, out TValue value)
      {
        var dic = FindBucket(key);

        if (dic == null)
        {
          value = default(TValue);
          return false;
        }

        return dic.TryGetValue(key, out value);
      }

      #endregion
    }
  }
}
