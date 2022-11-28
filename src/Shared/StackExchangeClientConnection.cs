//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.Diagnostics;
using System.IO;
using System.Web.SessionState;
using StackExchange.Redis;

namespace Microsoft.Web.Redis
{
    internal class StackExchangeClientConnection : IRedisClientConnection
    {
        private ProviderConfiguration _configuration;
        private RedisSharedConnection _sharedConnection;

        public StackExchangeClientConnection(ProviderConfiguration configuration, RedisSharedConnection sharedConnection)
        {
            _configuration = configuration;
            _sharedConnection = sharedConnection;
        }

        // This is used just by tests
        public IDatabase RealConnection
        {
            get { return _sharedConnection.Connection; }
        }

        public bool Expiry(string key, int timeInSeconds)
        {
            TimeSpan timeSpan = new TimeSpan(0, 0, timeInSeconds);
            RedisKey redisKey = key;
            return (bool)RetryLogic(() => RealConnection.KeyExpire(redisKey, timeSpan));
        }

        private object OperationExecutor(Func<object> redisOperation)
        {
            try
            {
                return redisOperation.Invoke();
            }
            catch (ObjectDisposedException)
            {
                // Try once as this can be caused by force reconnect by closing multiplexer
                return redisOperation.Invoke();
            }
            catch (RedisConnectionException)
            {
                // Try once after reconnect
                _sharedConnection.ForceReconnect();
                return redisOperation.Invoke();
            }
            catch (Exception e)
            {
                if (e.Message.Contains("NOSCRIPT"))
                {
                    // Second call should pass if it was script not found issue
                    return redisOperation.Invoke();
                }
                throw;
            }
        }

        /// <summary>
        /// If retry timout is provide than we will retry first time after 20 ms and after that every 1 sec till retry timout is expired or we get value.
        /// </summary>
        private object RetryLogic(Func<object> redisOperation)
        {
            int timeToSleepBeforeRetryInMiliseconds = 20;
            DateTime startTime = DateTime.Now;
            while (true)
            {
                try
                {
                    return OperationExecutor(redisOperation);
                }
                catch (Exception e)
                {
                    TimeSpan passedTime = DateTime.Now - startTime;
                    if (_configuration.RetryTimeout < passedTime)
                    {
                        LogUtility.LogError($"Exception: {e.Message}");
                        throw;
                    }
                    else
                    {
                        int remainingTimeout = (int)(_configuration.RetryTimeout.TotalMilliseconds - passedTime.TotalMilliseconds);
                        // if remaining time is less than 1 sec than wait only for that much time and than give a last try
                        if (remainingTimeout < timeToSleepBeforeRetryInMiliseconds)
                        {
                            timeToSleepBeforeRetryInMiliseconds = remainingTimeout;
                        }
                    }

                    // First time try after 20 msec after that try after 1 second
                    System.Threading.Thread.Sleep(timeToSleepBeforeRetryInMiliseconds);
                    timeToSleepBeforeRetryInMiliseconds = 1000;
                }
            }
        }

        public void Set(string key, byte[] data, DateTime utcExpiry)
        {
            RedisKey redisKey = key;
            RedisValue redisValue = data;
            TimeSpan timeSpanForExpiry = utcExpiry - DateTime.UtcNow;
            OperationExecutor(() => RealConnection.StringSet(redisKey, redisValue, timeSpanForExpiry));
        }

        public void SetInt(string key, int data, DateTime utcExpiry)
        {
            RedisKey redisKey = key;
            RedisValue redisValue = data;
            TimeSpan timeSpanForExpiry = utcExpiry - DateTime.UtcNow;
            OperationExecutor(() => RealConnection.StringSet(redisKey, redisValue, timeSpanForExpiry));
        }

        public void SetString(string key, string data, int timeInSeconds)
        {
            RedisKey redisKey = key;
            RedisValue redisValue = data;
            TimeSpan timeSpanForExpiry = new TimeSpan(0, 0, timeInSeconds);
            OperationExecutor(() => RealConnection.StringSet(redisKey, redisValue, timeSpanForExpiry));
        }

        public byte[] Get(string key)
        {
            RedisKey redisKey = key;
            RedisValue redisValue = (RedisValue)OperationExecutor(() => RealConnection.StringGet(redisKey));
            return (byte[])redisValue;
        }

        public string GetString(string key)
        {
            RedisKey redisKey = key;
            RedisValue redisValue = (RedisValue)OperationExecutor(() => RealConnection.StringGet(redisKey));
            return redisValue;
        }

        public void Remove(string key)
        {
            RedisKey redisKey = key;
            OperationExecutor(() => RealConnection.KeyDelete(redisKey));
        }
    }
}