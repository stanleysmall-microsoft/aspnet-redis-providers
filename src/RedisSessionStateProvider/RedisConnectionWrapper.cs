//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.IO;
using System.Web.SessionState;

namespace Microsoft.Web.Redis
{
    internal class RedisConnectionWrapper : ICacheConnection
    {
        internal static RedisSharedConnection sharedConnection;
        private static readonly object lockForSharedConnection = new object();

        public KeyGenerator Keys { set; get; }

        internal IRedisClientConnection redisConnection;
        private readonly ProviderConfiguration configuration;

        public RedisConnectionWrapper(ProviderConfiguration configuration, string id)
        {
            this.configuration = configuration;
            Keys = new KeyGenerator(id, configuration.ApplicationName);

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

        public TimeSpan GetLockAge(object lockId)
        {
            // This method do not use redis
            string lockDateTimeTicksFromLockId = lockId.ToString();
            if (long.TryParse(lockDateTimeTicksFromLockId, out long lockTimeTicks))
            {
                return DateTime.Now.Subtract(new DateTime(lockTimeTicks));
            }
            else
            { //lock id is not valid so release item exclusive should be called so make lock age very large
                return DateTime.Now.Subtract(new DateTime());
            }
        }

        public void UpdateExpiryTime(int timeToExpireInSeconds)
        {
            redisConnection.Expiry(Keys.DataKey, timeToExpireInSeconds);
            redisConnection.Expiry(Keys.InternalKey, timeToExpireInSeconds);
        }

        public void Set(ISessionStateItemCollection data, int sessionTimeout)
        {
            var expiry = DateTime.UtcNow.AddSeconds(sessionTimeout);
            try
            {
                byte[] serializedSessionStateItemCollection = SerializeSessionStateItemCollection(data);
                redisConnection.Set(Keys.DataKey, serializedSessionStateItemCollection, expiry);
                redisConnection.SetInt(Keys.InternalKey, sessionTimeout, expiry);
            }
            catch
            {
            }
        }

        public bool TryTakeWriteLockAndGetData(DateTime lockTime, int lockTimeout, out object lockId, out ISessionStateItemCollection data, out int sessionTimeout)
        {
            string expectedLockId = lockTime.Ticks.ToString();

            bool isLocked = true;
            lockId = redisConnection.GetString(Keys.LockKey) ?? "";
            if (lockId.ToString().Equals(""))
            {
                isLocked = false;
                redisConnection.SetString(Keys.LockKey, expectedLockId, lockTimeout);
            }

            bool ret = false;
            data = null;

            lockId = redisConnection.GetString(Keys.LockKey) ?? "";
            var result = redisConnection.GetString(Keys.InternalKey) ?? "0";
            sessionTimeout = int.Parse(result);
            if (!isLocked && lockId.ToString().Equals(expectedLockId))
            {
                ret = true;
                data = DeserializeSessionStateItemCollection(redisConnection.Get(Keys.DataKey));
            }
            return ret;
        }

        public bool TryCheckWriteLockAndGetData(out object lockId, out ISessionStateItemCollection data, out int sessionTimeout)
        {
            bool ret = false;
            data = null;

            lockId = redisConnection.GetString(Keys.LockKey) ?? "";
            var result = redisConnection.GetString(Keys.InternalKey) ?? "0";
            sessionTimeout = int.Parse(result);
            if (lockId.ToString().Equals(""))
            {
                ret = true;
                data = DeserializeSessionStateItemCollection(redisConnection.Get(Keys.DataKey));
                lockId = null;
            }
            return ret;
        }

        public void TryReleaseLockIfLockIdMatch(object lockId, int sessionTimeout)
        {
            lockId = lockId ?? "";
            var realLockId = redisConnection.GetString(Keys.LockKey) ?? "";

            if (realLockId.Equals(lockId.ToString()))
            {
                redisConnection.Remove(Keys.LockKey);

                var result = redisConnection.GetString(Keys.InternalKey) ?? "0";
                var realSessionTimeout = int.Parse(result);

                if (realSessionTimeout != 0)
                {
                    redisConnection.Expiry(Keys.DataKey, realSessionTimeout);
                    redisConnection.Expiry(Keys.InternalKey, realSessionTimeout);
                }
                else
                {
                    redisConnection.Expiry(Keys.DataKey, sessionTimeout);
                    redisConnection.Expiry(Keys.InternalKey, sessionTimeout);
                }
            }
        }

        public void TryRemoveAndReleaseLock(object lockId)
        {
            lockId = lockId ?? "";
            var realLockId = redisConnection.GetString(Keys.LockKey) ?? "";

            if (realLockId.Equals(lockId.ToString()))
            {
                redisConnection.Remove(Keys.LockKey);
                redisConnection.Remove(Keys.DataKey);
                redisConnection.Remove(Keys.InternalKey);
            }
        }

        public void TryUpdateAndReleaseLock(object lockId, ISessionStateItemCollection data, int sessionTimeout)
        {
            lockId = lockId ?? "";
            var realLockId = redisConnection.GetString(Keys.LockKey) ?? "";

            if (realLockId.Equals(lockId.ToString()))
            {
                Set(data, sessionTimeout);
                redisConnection.Remove(Keys.LockKey);
            }
        }

        private byte[] SerializeSessionStateItemCollection(ISessionStateItemCollection sessionStateItemCollection)
        {
            try
            {
                MemoryStream ms = new MemoryStream();
                BinaryWriter writer = new BinaryWriter(ms);
                ((SessionStateItemCollection)sessionStateItemCollection).Serialize(writer);
                writer.Close();
                return ms.ToArray();
            }
            catch
            {
                return null;
            }
        }

        private SessionStateItemCollection DeserializeSessionStateItemCollection(byte[] serializedSessionStateItemCollection)
        {
            try
            {
                MemoryStream ms = new MemoryStream(serializedSessionStateItemCollection);
                BinaryReader reader = new BinaryReader(ms);
                return SessionStateItemCollection.Deserialize(reader);
            }
            catch
            {
                return null;
            }
        }
    }
}