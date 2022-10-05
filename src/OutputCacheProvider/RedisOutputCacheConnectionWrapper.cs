//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.IO;
using System.Web.Caching;

namespace Microsoft.Web.Redis
{
    internal class RedisOutputCacheConnectionWrapper : IOutputCacheConnection
    {
        internal static RedisSharedConnection sharedConnection;
        private static object lockForSharedConnection = new object();

        internal IRedisClientConnection redisConnection;
        private ProviderConfiguration configuration;

        public RedisOutputCacheConnectionWrapper(ProviderConfiguration configuration)
        {
            this.configuration = configuration;

            // only single object of RedisSharedConnection will be created and then reused
            if (sharedConnection == null)
            {
                lock (lockForSharedConnection)
                {
                    if (sharedConnection == null)
                    {
                        sharedConnection = new RedisSharedConnection(configuration);
                    }
                }
            }
            redisConnection = new StackExchangeClientConnection(configuration, sharedConnection);
        }

        public object Add(string key, object entry, DateTime utcExpiry)
        {
            key = GetKeyForRedis(key);

            var result = redisConnection.Get(key);
            if (result != null)
            {
                return DeserializeOutputCacheEntry(result);
            }
            else
            {
                redisConnection.Set(key, SerializeOutputCacheEntry(entry), utcExpiry);
                return entry;
            }
        }

        public void Set(string key, object entry, DateTime utcExpiry)
        {
            key = GetKeyForRedis(key);
            byte[] data = SerializeOutputCacheEntry(entry);

            redisConnection.Set(key, data, utcExpiry);
        }

        public object Get(string key)
        {
            key = GetKeyForRedis(key);

            byte[] data = redisConnection.Get(key);
            return DeserializeOutputCacheEntry(data);
        }

        public void Remove(string key)
        {
            key = GetKeyForRedis(key);
            redisConnection.Remove(key);
        }

        private string GetKeyForRedis(string key)
        {
            return configuration.ApplicationName + "_" + key;
        }

        private byte[] SerializeOutputCacheEntry(object outputCacheEntry)
        {
            try
            {
                MemoryStream ms = new MemoryStream();
                OutputCache.Serialize(ms, outputCacheEntry);
                return ms.ToArray();
            }
            catch (ArgumentException)
            {
                LogUtility.LogWarning("{0} is not one of the specified output-cache types.", outputCacheEntry);
                return null;
            }
        }

        private object DeserializeOutputCacheEntry(byte[] serializedOutputCacheEntry)
        {
            try
            {
                MemoryStream ms = new MemoryStream(serializedOutputCacheEntry);
                return OutputCache.Deserialize(ms);
            }
            catch (ArgumentException)
            {
                LogUtility.LogWarning("The output cache entry is not one of the specified output-cache types.");
                return null;
            }
        }
    }
}