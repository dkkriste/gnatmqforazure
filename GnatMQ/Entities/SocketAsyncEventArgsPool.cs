namespace GnatMQForAzure.Entities
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Net.Sockets;
    using System.Threading;

    public class SocketAsyncEventArgsPool
    {
        // Pool of reusable SocketAsyncEventArgs objects.
        private readonly ConcurrentStack<SocketAsyncEventArgs> pool;

        internal SocketAsyncEventArgsPool()
        {
            this.pool = new ConcurrentStack<SocketAsyncEventArgs>();
        }

        // The number of SocketAsyncEventArgs instances in the pool.
        internal int Count => this.pool.Count;

        // Removes a SocketAsyncEventArgs instance from the pool.
        // returns SocketAsyncEventArgs removed from the pool.
        internal SocketAsyncEventArgs Pop()
        {
            SocketAsyncEventArgs args;
            if (pool.TryPop(out args))
            {
                return args;
            }

            return null;
        }

        internal void Push(SocketAsyncEventArgs item)
        {
            this.pool.Push(item);
        }
    }
}