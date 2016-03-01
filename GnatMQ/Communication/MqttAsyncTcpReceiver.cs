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
            bool willRaiseEvent = receiveEventArgs.AcceptSocket.ReceiveAsync(receiveEventArgs);
            if (!willRaiseEvent)
            {
                ProcessReceive(receiveEventArgs);
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

            //TODO signal close
        }

        private void TryProcessMessage(MqttClientConnection connection, SocketAsyncEventArgs receiveSendEventArgs)
        {
            int lastProcessedByteByCompleteMessage = -1;
            int remainingBytesToProcess = connection.PreviouslyRead + receiveSendEventArgs.BytesTransferred;
            var bufferOffset = connection.ReceiveSocketOffset;

            while (remainingBytesToProcess > 0)
            {
                try
                {
                    var messageType = GetMessageType(receiveSendEventArgs.Buffer, ref bufferOffset, ref remainingBytesToProcess);
                    var payloadLength = GetPayloadLength(receiveSendEventArgs.Buffer, ref bufferOffset, ref remainingBytesToProcess);
                    if (payloadLength <= remainingBytesToProcess)
                    {
                        var rawMessage = rawMessageManager.GetRawMessageWithData(messageType, receiveSendEventArgs.Buffer, bufferOffset, payloadLength);

                        // TODO enqueue raw message
                        bufferOffset += payloadLength;
                        remainingBytesToProcess -= payloadLength;
                        lastProcessedByteByCompleteMessage = bufferOffset;
                    }
                    else
                    {
                        throw new AggregateException();
                    }
                }
                catch (AggregateException)
                {
                    var unprocessedStart = lastProcessedByteByCompleteMessage + 1;
                    var totalUnprocessedBytes = (connection.ReceiveSocketOffset + connection.PreviouslyRead + receiveSendEventArgs.BytesTransferred) - unprocessedStart;
                    if (lastProcessedByteByCompleteMessage > 0)
                    {
                        Buffer.BlockCopy(receiveSendEventArgs.Buffer, connection.ReceiveSocketOffset + unprocessedStart, receiveSendEventArgs.Buffer, connection.ReceiveSocketOffset, totalUnprocessedBytes);
                    }

                    receiveSendEventArgs.SetBuffer(connection.ReceiveSocketOffset + totalUnprocessedBytes, connection.ReceiveSocketBufferSize - totalUnprocessedBytes);
                    connection.PreviouslyRead = totalUnprocessedBytes;
                    return;
                }
            }

            receiveSendEventArgs.SetBuffer(connection.ReceiveSocketOffset, connection.ReceiveSocketBufferSize);
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