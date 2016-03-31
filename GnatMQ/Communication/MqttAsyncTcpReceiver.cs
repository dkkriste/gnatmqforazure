namespace GnatMQForAzure.Communication
{
    using System;
    using System.Net.Sockets;

    using GnatMQForAzure.Managers;

    public class MqttAsyncTcpReceiver
    {
        private readonly MqttRawMessageManager rawMessageManager;

        public MqttAsyncTcpReceiver(MqttRawMessageManager rawMessageManager)
        {
            this.rawMessageManager = rawMessageManager;
        }

        public void StartReceive(SocketAsyncEventArgs receiveEventArgs)
        {
            try
            {
                bool willRaiseEvent = receiveEventArgs.AcceptSocket.ReceiveAsync(receiveEventArgs);
                if (!willRaiseEvent)
                {
                    ProcessReceive(receiveEventArgs);
                }
            }
            catch (Exception)
            {
            }
        }

        public void ReceiveCompleted(object sender, SocketAsyncEventArgs e)
        {
            ProcessReceive(e);
        }

        private void ProcessReceive(SocketAsyncEventArgs receiveSendEventArgs)
        {
            var clientConnection = (MqttClientConnection)receiveSendEventArgs.UserToken;

            if (receiveSendEventArgs.SocketError == SocketError.OperationAborted)
            {
                return;
            }
            else if (receiveSendEventArgs.SocketError != SocketError.Success)
            {
                CloseClientSocket(receiveSendEventArgs);
                return;
            }
            else if (receiveSendEventArgs.BytesTransferred == 0)
            {
                CloseClientSocket(receiveSendEventArgs);
                return;
            }
            else
            {
                // We got at least one byte
                TryProcessMessage(clientConnection, receiveSendEventArgs);
            }

            StartReceive(receiveSendEventArgs);
        }

        private void CloseClientSocket(SocketAsyncEventArgs e)
        {
            try
            {
                e.AcceptSocket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception)
            {
            }

            e.AcceptSocket.Close();

            var clientConnection = (MqttClientConnection)e.UserToken;
            clientConnection.OnConnectionClosed();
        }

        private void TryProcessMessage(MqttClientConnection clientConnection, SocketAsyncEventArgs receiveSendEventArgs)
        {
            int lastProcessedByteByCompleteMessage = -1;
            int remainingBytesToProcess = clientConnection.PreviouslyReceivedBytes + receiveSendEventArgs.BytesTransferred;
            var bufferOffset = clientConnection.ReceiveSocketOffset;

            while (remainingBytesToProcess > 0)
            {
                try
                {
                    var messageType = GetMessageType(receiveSendEventArgs.Buffer, ref bufferOffset, ref remainingBytesToProcess);
                    var payloadLength = GetPayloadLength(receiveSendEventArgs.Buffer, ref bufferOffset, ref remainingBytesToProcess);
                    if (payloadLength <= remainingBytesToProcess)
                    {
                        var rawMessage = rawMessageManager.GetRawMessageWithData(clientConnection, messageType, receiveSendEventArgs.Buffer, bufferOffset, payloadLength);
                        clientConnection.EnqueueRawMessage(rawMessage);
                        bufferOffset += payloadLength;
                        remainingBytesToProcess -= payloadLength;
                        lastProcessedByteByCompleteMessage = bufferOffset - clientConnection.ReceiveSocketOffset;
                    }
                    else
                    {
                        throw new AggregateException();
                    }
                }
                catch (AggregateException)
                {
                    var unprocessedStart = lastProcessedByteByCompleteMessage + 1;
                    var totalUnprocessedBytes = (clientConnection.PreviouslyReceivedBytes + receiveSendEventArgs.BytesTransferred) - unprocessedStart;
                    if (lastProcessedByteByCompleteMessage > 0)
                    {
                        Buffer.BlockCopy(receiveSendEventArgs.Buffer, clientConnection.ReceiveSocketOffset + unprocessedStart, receiveSendEventArgs.Buffer, clientConnection.ReceiveSocketOffset, totalUnprocessedBytes);
                    }

                    receiveSendEventArgs.SetBuffer(clientConnection.ReceiveSocketOffset + totalUnprocessedBytes, clientConnection.ReceiveSocketBufferSize - totalUnprocessedBytes);
                    clientConnection.PreviouslyReceivedBytes = totalUnprocessedBytes;
                    return;
                }
            }

            receiveSendEventArgs.SetBuffer(clientConnection.ReceiveSocketOffset, clientConnection.ReceiveSocketBufferSize);
        }

        private byte GetMessageType(byte[] buffer, ref int offset, ref int remainingBytesToProcess)
        {
            if (remainingBytesToProcess > 0)
            {
                var messageType = buffer[offset++];
                remainingBytesToProcess--;
                return messageType;
            }

            throw new AggregateException();
        }

        private int GetPayloadLength(byte[] buffer, ref int offset, ref int remainingBytesToProcess)
        {
            if (remainingBytesToProcess > 0)
            {
                int multiplier = 1;
                int value = 0;
                int digit = 0;
                do
                {
                    digit = buffer[offset];
                    value += (digit & 127) * multiplier;
                    multiplier *= 128;
                    offset++;
                    remainingBytesToProcess--;
                }
                while ((digit & 128) != 0 && remainingBytesToProcess > 0);

                if ((digit & 128) == 0)
                {
                    return value;
                }
            }

            throw new AggregateException();
        }
    }
}