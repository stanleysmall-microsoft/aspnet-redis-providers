//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Web.SessionState;

namespace Microsoft.Web.Redis
{
    internal class RedisConnectionWrapper : ICacheConnection
    {
        internal static RedisSharedConnection sharedConnection;
        private static object lockForSharedConnection = new object();

        public KeyGenerator Keys { set; get; }

        internal IRedisClientConnection redisConnection;
        private ProviderConfiguration configuration;

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
            long lockTimeTicks;
            if (long.TryParse(lockDateTimeTicksFromLockId, out lockTimeTicks))
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
            string[] keyArgs = new string[] { Keys.LockKey, Keys.DataKey, Keys.InternalKey };
            object[] valueArgs = new object[] { 0, 0 };

            object rowDataFromRedis = redisConnection.Eval(writeLockAndGetDataScript, keyArgs, valueArgs);
            var sessionTimeout = redisConnection.GetSessionTimeout(rowDataFromRedis);
            redisConnection.Expiry(Keys.DataKey, sessionTimeout);
            redisConnection.Expiry(Keys.InternalKey, sessionTimeout);
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

        /*-------Start of Lock set operation-----------------------------------------------------------------------------------------------------------------------------------------------*/

        // KEYS = { write-lock-id, data-id, internal-id }
        // ARGV = { write-lock-value-that-we-want-to-set, request-timout }
        // lockValue = 1) (Initially) write lock value that we want to set (ARGV[1]) if we get lock successfully this will return as retArray[1]
        //             2) If another write lock exists than its lock value from cache
        // retArray = {lockValue , session data if lock was taken successfully, session timeout value if exists, wheather lock was taken or not}
        private static readonly string writeLockAndGetDataScript = (@"
                local retArray = {}
                local lockValue = ARGV[1]
                local locked = redis.call('SETNX',KEYS[1],ARGV[1])
                local IsLocked = true

                if locked == 0 then
                    lockValue = redis.call('GET',KEYS[1])
                else
                    redis.call('EXPIRE',KEYS[1],ARGV[2])
                    IsLocked = false
                end

                retArray[1] = lockValue
                if lockValue == ARGV[1] then retArray[2] = redis.call('GET',KEYS[2]) else retArray[2] = '' end

                local SessionTimeout = redis.call('GET',KEYS[3])
                if SessionTimeout ~= false then
                    retArray[3] = SessionTimeout
                    redis.call('EXPIRE',KEYS[2], SessionTimeout)
                    redis.call('EXPIRE',KEYS[3], SessionTimeout)
                else
                    retArray[3] = '-1'
                end

                retArray[4] = IsLocked
                return retArray
                ");

        public bool TryTakeWriteLockAndGetData(DateTime lockTime, int lockTimeout, out object lockId, out ISessionStateItemCollection data, out int sessionTimeout)
        {
            string expectedLockId = lockTime.Ticks.ToString();
            string[] keyArgs = new string[] { Keys.LockKey, Keys.DataKey, Keys.InternalKey };
            object[] valueArgs = new object[] { expectedLockId, lockTimeout };

            object rowDataFromRedis = redisConnection.Eval(writeLockAndGetDataScript, keyArgs, valueArgs);

            bool ret = false;
            data = null;

            lockId = redisConnection.GetLockId(rowDataFromRedis);
            sessionTimeout = redisConnection.GetSessionTimeout(rowDataFromRedis);
            bool isLocked = redisConnection.IsLocked(rowDataFromRedis);
            if (!isLocked && lockId.ToString().Equals(expectedLockId))
            {
                ret = true;
                data = redisConnection.GetSessionData(rowDataFromRedis);
            }
            return ret;
        }

        // KEYS = { write-lock-id, data-id, internal-id }
        // ARGV = { }
        // lockValue = 1) (Initially) read lock value that we want to set (ARGV[1]) if we get lock successfully this will return as retArray[1]
        //             3) If write lock exists than its lock value from cache
        // retArray = {lockValue , session data if lock does not exist}
        private static readonly string readLockAndGetDataScript = (@"
                    local retArray = {}
                    local lockValue = ''
                    local writeLockValue = redis.call('GET',KEYS[1])
                    if writeLockValue ~= false then
                       lockValue = writeLockValue
                    end
                    retArray[1] = lockValue
                    if lockValue == '' then retArray[2] = redis.call('GET',KEYS[2]) else retArray[2] = '' end

                    local SessionTimeout = redis.call('GET', KEYS[3])
                    if SessionTimeout ~= false then
                        retArray[3] = SessionTimeout
                        redis.call('EXPIRE',KEYS[2], SessionTimeout)
                        redis.call('EXPIRE',KEYS[3], SessionTimeout)
                    else
                        retArray[3] = '-1'
                    end
                    return retArray
                    ");

        public bool TryCheckWriteLockAndGetData(out object lockId, out ISessionStateItemCollection data, out int sessionTimeout)
        {
            string[] keyArgs = new string[] { Keys.LockKey, Keys.DataKey, Keys.InternalKey };
            object[] valueArgs = new object[] { };

            object rowDataFromRedis = redisConnection.Eval(readLockAndGetDataScript, keyArgs, valueArgs);

            bool ret = false;
            data = null;

            lockId = LockId();
            sessionTimeout = SessionTimeout();
            if (lockId.ToString().Equals(""))
            {
                ret = true;
                data = redisConnection.GetSessionData(rowDataFromRedis);
            }
            return ret;
        }

        /*-------End of Lock set operation-----------------------------------------------------------------------------------------------------------------------------------------------*/

        public void TryReleaseLockIfLockIdMatch(object lockId, int sessionTimeout)
        {
            lockId = lockId ?? "";

            if (LockId().Equals(lockId.ToString()))
            {
                redisConnection.Remove(Keys.LockKey);
                if (SessionTimeout() != 0)
                {
                    redisConnection.Expiry(Keys.DataKey, SessionTimeout());
                    redisConnection.Expiry(Keys.InternalKey, SessionTimeout());
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

            if (LockId().Equals(lockId.ToString()))
            {
                redisConnection.Remove(Keys.LockKey);
                redisConnection.Remove(Keys.DataKey);
                redisConnection.Remove(Keys.InternalKey);
            }
        }

        public void TryUpdateAndReleaseLock(object lockId, ISessionStateItemCollection data, int sessionTimeout)
        {
            lockId = lockId ?? "";

            if (LockId().Equals(lockId.ToString()))
            {
                Set(data, sessionTimeout);
                redisConnection.Remove(Keys.LockKey);
            }
        }

        private string LockId()
        {
            string[] keyArgs = new string[] { Keys.LockKey, Keys.DataKey, Keys.InternalKey };
            object[] valueArgs = new object[] { };

            object rowDataFromRedis = redisConnection.Eval(readLockAndGetDataScript, keyArgs, valueArgs);

            return redisConnection.GetLockId(rowDataFromRedis);
        }

        private int SessionTimeout()
        {
            string[] keyArgs = new string[] { Keys.LockKey, Keys.DataKey, Keys.InternalKey };
            object[] valueArgs = new object[] { };

            object rowDataFromRedis = redisConnection.Eval(readLockAndGetDataScript, keyArgs, valueArgs);

            return redisConnection.GetSessionTimeout(rowDataFromRedis);
        }
    }
}