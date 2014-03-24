/*
 * Copyright (c) 2013, Allen A. George <allen dot george at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of libraft nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.libraft.kayvee.store;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import io.libraft.Committed;
import io.libraft.CommittedCommand;
import io.libraft.NotLeaderException;
import io.libraft.RaftListener;
import io.libraft.Snapshot;
import io.libraft.SnapshotRequest;
import io.libraft.agent.RaftAgent;
import io.libraft.algorithm.StorageException;
import io.libraft.kayvee.api.KeyValue;
import io.libraft.kayvee.api.SetValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * Interface to the Raft cluster.
 * <p/>
 * Contains an instance of {@link RaftAgent} and uses it to replicate
 * {@link KayVeeCommand} instances to the Raft cluster. When these instances
 * are committed they are locally applied to {@link LocalStore}. Since
 * each server in the cluster applies the committed {@code KayVeeCommand}
 * to {@code LocalStore} this transforms the cluster's distributed key-value state.
 * <p/>
 * This component is thread-safe.
 */
public class DistributedStore implements Managed, RaftListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStore.class);
    private static final Histogram SUBMIT_COMMAND_HISTOGRAM = Metrics.newHistogram(DistributedStore.class, "submit-command");
    private static final Histogram APPLY_COMMAND_HISTOGRAM = Metrics.newHistogram(DistributedStore.class, "apply-command");

    private class PendingCommandData {

        private final SettableFuture<?> commandFuture;
        private final long startTime;

        private PendingCommandData(SettableFuture<?> commandFuture) {
            this.commandFuture = commandFuture;
            this.startTime = System.currentTimeMillis();
        }

        public SettableFuture<?> getCommandFuture() {
            return commandFuture;
        }

        public long getStartTime() {
            return startTime;
        }
    }

    private final ConcurrentMap<Long, PendingCommandData> pendingCommands = Maps.newConcurrentMap();
    private final Random random = new Random();
    private final LocalStore localStore;

    private volatile boolean running; // set during start/stop and accessed by multiple threads

    // these values are set _once_ during startup
    // safe to access by multiple threads following call to start()
    private RaftAgent raftAgent;
    private boolean initialized;

    public DistributedStore(LocalStore localStore) {
        this.localStore = localStore;
    }

    public void setRaftAgent(RaftAgent raftAgent) {
        checkState(this.raftAgent == null);
        this.raftAgent = raftAgent;
    }

    public synchronized void initialize() throws StorageException, IOException {
        checkState(raftAgent != null);
        checkState(!initialized);

        raftAgent.initialize();
        locallyApplyUnappliedCommittedState();

        initialized = true;
    }

    private void locallyApplyUnappliedCommittedState() throws IOException {
        while(true) {
            Committed committed = raftAgent.getNextCommitted(localStore.getLastAppliedIndex());

            if (committed == null) {
                break;
            }

            if (committed.getType() == Committed.Type.SNAPSHOT) {
                Snapshot snapshot = (Snapshot) committed;
                localStore.loadSnapshotFrom(snapshot.getIndex(), snapshot.getSnapshotInputStream());
            } else if (committed.getType() == Committed.Type.LOGENTRY) {
                CommittedCommand committedCommand = (CommittedCommand) committed;
                applyCommandInternal(committedCommand.getIndex(), (KayVeeCommand) committedCommand.getCommand());
            } else {
                throw new IllegalArgumentException("unrecognized Committed type:" + committed.getType().name());
            }
        }
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }

        checkState(raftAgent != null);
        checkState(initialized);

        raftAgent.start();

        running = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        if (!running) {
            return;
        }

        raftAgent.stop();

        running = false;
    }

    // IMPORTANT: DO NOT HOLD A LOCK WHEN CALLING issueCommandToCluster OR IN THE onLeadershipChange CALLBACK

    //----------------------------------------------------------------------------------------------------------------//
    //
    // snapshots
    //

    @Override
    public void createSnapshot(SnapshotRequest snapshotRequest) {
        try {
            long lastAppliedIndex = localStore.dumpSnapshotTo(snapshotRequest.getSnapshotOutputStream());
            snapshotRequest.setIndex(lastAppliedIndex);
            raftAgent.snapshotCreatedFor(snapshotRequest);
        } catch (IOException e) {
            LOGGER.warn("fail create snapshot:{}", snapshotRequest);
            throw new IllegalStateException("failed to create a snapshot", e);
        }
    }

    @Override
    public void applySnapshot(Snapshot snapshot) {
        try {
            localStore.loadSnapshotFrom(snapshot.getIndex(), snapshot.getSnapshotInputStream());
        } catch (IOException e) {
            LOGGER.error("fail apply snapshot:{}", snapshot);
            throw new IllegalStateException("failed to apply a snapshot", e);
        }
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // cluster state changes
    //

    @Override
    public void onLeadershipChange(@Nullable String leader) {
        // noop - I don't care
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // cluster-committed
    //

    @Override
    public void applyCommand(CommittedCommand committedCommand) {
        long index = committedCommand.getIndex();
        KayVeeCommand command = (KayVeeCommand) committedCommand.getCommand();

        if (!running) {
            LOGGER.warn("store no longer active - not applying {} at index {}", command, index);
            return;
        }

        applyCommandInternal(index, command);
    }

    private void applyCommandInternal(long index, KayVeeCommand kayVeeCommand) {
        SettableFuture<?> removed;

        // check if this command has not failed already
        PendingCommandData pendingCommandData = pendingCommands.remove(kayVeeCommand.getCommandId());
        if (pendingCommandData != null) {
            removed = pendingCommandData.getCommandFuture();
        } else {
            removed = SettableFuture.create(); // create a fake future just to avoid if (removed ...)
        }

        // metrics
        long applyCommandInternalStartTime = System.currentTimeMillis();
        if (pendingCommandData != null) {
            SUBMIT_COMMAND_HISTOGRAM.update(applyCommandInternalStartTime - pendingCommandData.getStartTime());
        }

        // actually apply the command
        try {
            LOGGER.info("apply {} at index {}", kayVeeCommand, index);

            switch (kayVeeCommand.getType()) {
                case NOP:
                    applyNOPCommand(index, removed);
                    break;
                case GET:
                    KayVeeCommand.GETCommand getCommand = (KayVeeCommand.GETCommand) kayVeeCommand;
                    applyGETCommand(index, getCommand, removed);
                    break;
                case ALL:
                    applyALLCommand(index, removed);
                    break;
                case SET:
                    KayVeeCommand.SETCommand setCommand = (KayVeeCommand.SETCommand) kayVeeCommand;
                    applySETCommand(index, setCommand, removed);
                    break;
                case CAS:
                    KayVeeCommand.CASCommand casCommand = (KayVeeCommand.CASCommand) kayVeeCommand;
                    applyCASCommand(index, casCommand, removed);
                    break;
                case DEL:
                    KayVeeCommand.DELCommand delCommand = (KayVeeCommand.DELCommand) kayVeeCommand;
                    applyDELCommand(index, delCommand, removed);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported type:" + kayVeeCommand.getType().name());
            }
        } catch (Exception e) {
            removed.setException(e);
        } finally {
            if (pendingCommandData != null) {
                APPLY_COMMAND_HISTOGRAM.update(System.currentTimeMillis() - applyCommandInternalStartTime);
            }
        }
    }

    private void applyNOPCommand(long index, SettableFuture<?> removed) {
        localStore.nop(index);
        setRemoved(removed, null);
    }

    private void applyGETCommand(long index, KayVeeCommand.GETCommand getCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.get(index, getCommand.getKey());
        setRemoved(removed, keyValue);
    }

    private void applyALLCommand(long index, SettableFuture<?> removed) {
        Collection<KeyValue> keyValues = localStore.getAll(index);
        setRemoved(removed, keyValues);
    }

    private void applySETCommand(long index, KayVeeCommand.SETCommand setCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.set(index, setCommand.getKey(), setCommand.getNewValue());
        setRemoved(removed, keyValue);
    }

    private void applyCASCommand(long index, KayVeeCommand.CASCommand casCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.compareAndSet(index, casCommand.getKey(), casCommand.getExpectedValue(), casCommand.getNewValue());
        setRemoved(removed, keyValue);
    }

    private void applyDELCommand(long index, KayVeeCommand.DELCommand delCommand, SettableFuture<?> removed) throws KayVeeException {
        localStore.delete(index, delCommand.getKey());
        setRemoved(removed, null);
    }

    @SuppressWarnings("unchecked")
    private <T> void setRemoved(SettableFuture<?> removed, T value) {
        SettableFuture<T> commandFuture = (SettableFuture<T>) removed;
        commandFuture.set(value);
    }

    //----------------------------------------------------------------------------------------------------------------//

    // client-issued

    public ListenableFuture<Void> nop() {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.NOPCommand nopCommand = new KayVeeCommand.NOPCommand(getCommandId());
        return issueCommandToCluster(nopCommand);
    }

    public ListenableFuture<KeyValue> get(String key) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.GETCommand getCommand = new KayVeeCommand.GETCommand(getCommandId(), key);
        return issueCommandToCluster(getCommand);
    }

    public ListenableFuture<Collection<KeyValue>> getAll() {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.ALLCommand allCommand = new KayVeeCommand.ALLCommand(getCommandId());
        return issueCommandToCluster(allCommand);
    }

    public ListenableFuture<KeyValue> set(String key, SetValue setValue) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.SETCommand setCommand = new KayVeeCommand.SETCommand(getCommandId(), key, setValue.getNewValue());
        return issueCommandToCluster(setCommand);
    }

    public ListenableFuture<KeyValue> compareAndSet(String key, SetValue setValue) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.CASCommand casCommand = new KayVeeCommand.CASCommand(getCommandId(), key, setValue.getExpectedValue(), setValue.getNewValue());
        return issueCommandToCluster(casCommand);
    }

    public ListenableFuture<Void> delete(String key) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.DELCommand delCommand = new KayVeeCommand.DELCommand(getCommandId(), key);
        return issueCommandToCluster(delCommand);
    }

    private void checkThatDistributedStoreIsActive() {
        checkState(running);
    }

    private long getCommandId() {
        synchronized (random) {
            return random.nextLong();
        }
    }

    // IMPORTANT: DO NOT HOLD A LOCK WHEN CALLING issueCommandToCluster OR IN THE onLeadershipChange CALLBACK
    private <T> ListenableFuture<T> issueCommandToCluster(final KayVeeCommand kayVeeCommand) {
        final SettableFuture<T> returned = SettableFuture.create();

        try {
            PendingCommandData previous = pendingCommands.put(kayVeeCommand.getCommandId(), new PendingCommandData(returned));
            checkState(previous == null, "existing command:%s", previous);

            Futures.addCallback(returned, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                    // success handled in the apply command block
                }

                @Override
                public void onFailure(Throwable t) {
                    pendingCommands.remove(kayVeeCommand.getCommandId());
                }
            });

            // it's possible for this to throw an IllegalStateException
            // if another thread calls "stop" while a request is just about to be submitted
            // since access to raftAgent itself is not synchronized here
            ListenableFuture<Void> consensusFuture = raftAgent.submitCommand(kayVeeCommand);
            Futures.addCallback(consensusFuture, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    // noop - we wait until we attempt to apply the command locally
                }

                @Override
                public void onFailure(Throwable t) {
                    returned.setException(t);
                }
            });
        } catch (NotLeaderException e) {
            returned.setException(e);
        }

        return returned;
    }
}
