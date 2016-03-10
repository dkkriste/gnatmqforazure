namespace GnatMQForAzure.Communication
{
    using System;
    using System.Collections.Concurrent;
    using System.Net.Sockets;

    using GnatMQForAzure.Entities;

    public class MqttAsyncTcpSender
    {
        private readonly BufferManager sendBufferManager;

        private readonly ConcurrentStack<SocketAsyncEventArgs> sendBufferEventArgsPool;

        public MqttAsyncTcpSender(MqttOptions options)
        {
            sendBufferManager = new BufferManager(options.NumberOfSendBuffers, options.ReadAndSendBufferSize);
            sendBufferEventArgsPool = new ConcurrentStack<SocketAsyncEventArgs>();

            for (var i = 0; i < options.NumberOfSendBuffers; i++)
            {
                var args = CreateAndSetNewSendArgs();
                sendBufferEventArgsPool.Push(args);
            }
        }

        public void Send(Socket socket, byte[] message)
        {
            SocketAsyncEventArgs socketArgs;
            if (sendBufferEventArgsPool.TryPop(out socketArgs))
            {
                socketArgs.AcceptSocket = socket;
                Buffer.BlockCopy(message, 0, socketArgs.Buffer, socketArgs.Offset, message.Length);
                StartSend(socketArgs);
            }
            else
            {
                throw new Exception("No more SendArgs in pool");
            }
        }

        private SocketAsyncEventArgs CreateAndSetNewSendArgs()
        {
            var args = new SocketAsyncEventArgs();
            sendBufferManager.SetBuffer(args);
            args.Completed += SendCompleted;
            args.UserToken = new SendSocketArgs(args.Offset, args.Count);
            return args;
        }

        private void StartSend(SocketAsyncEventArgs sendEventArgs)
        {
            bool willRaiseEvent = sendEventArgs.AcceptSocket.SendAsync(sendEventArgs);
            if (!willRaiseEvent)
            {
                ProcessSend(sendEventArgs);
            }
        }

        private void SendCompleted(object sender, SocketAsyncEventArgs sendEventArgs)
        {
            ProcessSend(sendEventArgs);
        }

        private void ProcessSend(SocketAsyncEventArgs sendEventArgs)
        {
            if (sendEventArgs.SocketError == SocketError.OperationAborted)
            {
                return;
            }

            if (sendEventArgs.SocketError == SocketError.Success)
            {
                if (sendEventArgs.Count == sendEventArgs.BytesTransferred)
                {
                    // Send complete, reset and return to pool
                    var sendSocketArgs = (SendSocketArgs)sendEventArgs.UserToken;
                    sendEventArgs.AcceptSocket = null;
                    sendEventArgs.SetBuffer(sendSocketArgs.BufferOffset, sendSocketArgs.BufferSize);
                    sendBufferEventArgsPool.Push(sendEventArgs);
                }
                else
                {
                    //If some of the bytes in the message have NOT been sent,
                    //then we will need to post another send operation.
                    var sendSocketArgs = (SendSocketArgs)sendEventArgs.UserToken;
                    sendSocketArgs.MessageLength -= sendEventArgs.BytesTransferred;
                    sendSocketArgs.MessageStartFromOffset += sendEventArgs.BytesTransferred;
                    sendEventArgs.SetBuffer(sendSocketArgs.MessageStartFromOffset, sendSocketArgs.MessageLength);
                    bool willRaiseEvent = sendEventArgs.AcceptSocket.SendAsync(sendEventArgs);
                    if (!willRaiseEvent)
                    {
                        ProcessSend(sendEventArgs);
                    }
                }
            }
            else
            {
                CloseClientSocket(sendEventArgs);
            }
        }

        private void CloseClientSocket(SocketAsyncEventArgs sendEventArgs)
        {
            try
            {
                sendEventArgs.AcceptSocket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception)
            {
            }

            sendEventArgs.AcceptSocket.Close();

            var sendSocketArgs = (SendSocketArgs)sendEventArgs.UserToken;
            sendEventArgs.AcceptSocket = null;
            sendEventArgs.SetBuffer(sendSocketArgs.BufferOffset, sendSocketArgs.BufferSize);
            sendBufferEventArgsPool.Push(sendEventArgs);
        }
    }
}