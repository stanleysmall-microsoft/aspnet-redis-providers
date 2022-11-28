//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.Web.SessionState;

namespace Microsoft.Web.Redis
{
    internal interface IRedisClientConnection
    {
        bool Expiry(string key, int timeInSeconds);

        void Set(string key, byte[] data, DateTime utcExpiry);

        void SetInt(string key, int data, DateTime utcExpiry);

        void SetString(string key, string data, int timeInSeconds);

        byte[] Get(string key);

        string GetString(string key);

        void Remove(string key);
    }
}