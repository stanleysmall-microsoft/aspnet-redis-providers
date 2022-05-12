﻿//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;

namespace Microsoft.Web.Redis.FunctionalTests
{
    internal class RedisServer : IDisposable
    {
        Process _server;

        private static void WaitForRedisToStart()
        {
            // if redis is not up in 2 seconds time than return failure
            for (int i = 0; i < 200; i++)
            {
                Thread.Sleep(10);
                try
                {
                    Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect("localhost", 6379);
                    socket.Close();
                    LogUtility.LogInfo("Successful started redis server after Time: {0} ms", (i+1) * 10);
                    break;
                }
                catch
                {}
            }
        }

        public RedisServer()
        {
            _server = new Process();
            Restart();
        }

        public void Restart()
        {
            KillRedisServers();
            _server = new Process();
            string defaultExecutablePath = $"{Environment.CurrentDirectory}\\..\\..\\..\\..\\..\\packages\\redis-64\\3.0.503\\tools\\redis-server.exe";
            _server.StartInfo.FileName = Environment.GetEnvironmentVariable("REDIS_SERVER_PATH") ?? defaultExecutablePath;
            _server.StartInfo.Arguments = "--maxmemory 20000000";
            _server.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            _server.Start();
            WaitForRedisToStart();
        }

        // Make sure that no redis-server instance is running
        private static void KillRedisServers()
        {
            foreach (var proc in Process.GetProcessesByName("redis-server"))
            {
                try
                {
                    proc.Kill();
                }
                catch
                {
                }
            }
        }

        public void Dispose()
        {
            try
            {
                if (_server != null)
                {
                    _server.Kill();
                }
            }
            catch
            { }
        }
    }
}
