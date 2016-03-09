namespace GnatMQForAzure.Entities
{
    using System;
    using System.Collections.Generic;
    using System.Net.Sockets;
    using System.Threading;

    public class SocketAsyncEventArgsPool
    {
        //just for assigning an ID so we can watch our objects while testing.
        private int nextTokenId = 0;

        // Pool of reusable SocketAsyncEventArgs objects.
        Stack<SocketAsyncEventArgs> pool;

        // initializes the object pool to the specified size.
        // "capacity" = Maximum number of SocketAsyncEventArgs objects
        internal SocketAsyncEventArgsPool(int capacity)
        {
            this.pool = new Stack<SocketAsyncEventArgs>(capacity);
        }

        // The number of SocketAsyncEventArgs instances in the pool.
        internal int Count => this.pool.Count;

        // Removes a SocketAsyncEventArgs instance from the pool.
        // returns SocketAsyncEventArgs removed from the pool.
        internal SocketAsyncEventArgs Pop()
        {
            lock (this.pool)
            {
                return this.pool.Pop();
            }
        }

        // Add a SocketAsyncEventArg instance to the pool.
        // "item" = SocketAsyncEventArgs instance to add to the pool.
        internal void Push(SocketAsyncEventArgs item)
        {
            if (item == null)
            {
                throw new ArgumentNullException("Items added to a SocketAsyncEventArgsPool cannot be null");
            }
            lock (this.pool)
            {
                this.pool.Push(item);
            }
        }
    }
}