/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

package org.apache.cassandra.streaming;

/**
 * Handles the streaming a one or more section of one of more sstables to and from a specific
 * remote node.
 *
 * Both this node and the remote one will create a similar symmetrical StreamSession. A streaming
 * session has the following life-cycle:
 *
 * 1. Connections Initialization
 *
 *   (a) A node (the initiator in the following) create a new StreamSession, initialize it (init())
 *       and then start it (start()). Start will create a {@link ConnectionHandler} that will create
 *       two connections to the remote node (the follower in the following) with whom to stream and send
 *       a StreamInit message. The first connection will be the incoming connection for the
 *       initiator, and the second connection will be the outgoing.
 *   (b) Upon reception of that StreamInit message, the follower creates its own StreamSession,
 *       initialize it if it still does not exist, and attach connecting socket to its ConnectionHandler
 *       according to StreamInit message's isForOutgoing flag.
 *   (d) When the both incoming and outgoing connections are established, StreamSession calls
 *       StreamSession#onInitializationComplete method to start the streaming prepare phase
 *       (StreamResultFuture.startStreaming()).
 *
 * 2. Streaming preparation phase
 *
 *   (a) This phase is started when the initiator onInitializationComplete() method is called. This method sends a
 *       PrepareMessage that includes what files/sections this node will stream to the follower
 *       (stored in a StreamTransferTask, each column family has it's own transfer task) and what
 *       the follower needs to stream back (StreamReceiveTask, same as above). If the initiator has
 *       nothing to receive from the follower, it goes directly to its Streaming phase. Otherwise,
 *       it waits for the follower PrepareMessage.
 *   (b) Upon reception of the PrepareMessage, the follower records which files/sections it will receive
 *       and send back its own PrepareMessage with a summary of the files/sections that will be sent to
 *       the initiator (prepare()). After having sent that message, the follower goes to its Streamning
 *       phase.
 *   (c) When the initiator receives the follower PrepareMessage, it records which files/sections it will
 *       receive and then goes to his own Streaming phase.
 *
 * 3. Streaming phase
 *
 *   (a) The streaming phase is started by each node (the sender in the follower, but note that each side
 *       of the StreamSession may be sender for some of the files) involved by calling startStreamingFiles().
 *       This will sequentially send a FileMessage for each file of each SteamTransferTask. Each FileMessage
 *       consists of a FileMessageHeader that indicates which file is coming and then start streaming the
 *       content for that file (StreamWriter in FileMessage.serialize()). When a file is fully sent, the
 *       fileSent() method is called for that file. If all the files for a StreamTransferTask are sent
 *       (StreamTransferTask.complete()), the task is marked complete (taskCompleted()).
 *   (b) On the receiving side, a SSTable will be written for the incoming file (StreamReader in
 *       FileMessage.deserialize()) and once the FileMessage is fully received, the file will be marked as
 *       complete (received()). When all files for the StreamReceiveTask have been received, the sstables
 *       are added to the CFS (and 2ndary index are built, StreamReceiveTask.complete()) and the task
 *       is marked complete (taskCompleted())
 *   (b) If during the streaming of a particular file an I/O error occurs on the receiving end of a stream
 *       (FileMessage.deserialize), the node will retry the file (up to DatabaseDescriptor.getMaxStreamingRetries())
 *       by sending a RetryMessage to the sender. On receiving a RetryMessage, the sender simply issue a new
 *       FileMessage for that file.
 *   (c) When all transfer and receive tasks for a session are complete, the move to the Completion phase
 *       (maybeCompleted()).
 *
 * 4. Completion phase
 *
 *   (a) When a node has finished all transfer and receive task, it enter the completion phase (maybeCompleted()).
 *       If it had already received a CompleteMessage from the other side (it is in the WAIT_COMPLETE state), that
 *       session is done is is closed (closeSession()). Otherwise, the node switch to the WAIT_COMPLETE state and
 *       send a CompleteMessage to the other side.
 */
public class StreamSession
{

    public static enum State
    {
        INITIALIZED,
        PREPARING,
        STREAMING,
        WAIT_COMPLETE,
        COMPLETE,
        FAILED,
    }


}
