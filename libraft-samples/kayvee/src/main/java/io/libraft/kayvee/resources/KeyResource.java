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

package io.libraft.kayvee.resources;

import com.google.common.util.concurrent.ListenableFuture;
import io.libraft.NotLeaderException;
import io.libraft.kayvee.api.KeyValue;
import io.libraft.kayvee.api.SetValue;
import io.libraft.kayvee.configuration.ClusterMember;
import io.libraft.kayvee.store.CannotSubmitCommandException;
import io.libraft.kayvee.store.DistributedStore;
import io.libraft.kayvee.store.KayVeeCommand;
import io.libraft.kayvee.store.KayVeeException;
import io.libraft.kayvee.store.KeyNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * JAX-RS-annotated class that represents a key-value pair
 * and a set of CRUD operations on it. Operations
 * on this resource will transform the replicated key-value
 * state by committing a {@link KayVeeCommand} to
 * the Raft cluster.
 * <p/>
 * The resource path is {@code http://base.url/keys/THE_KEY}
 */
public final class KeyResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyResource.class);

    private final String key;
    private final Set<ClusterMember> members;
    private final DistributedStore distributedStore;

    public KeyResource(String key, Set<ClusterMember> members, DistributedStore distributedStore) {
        this.key = key;
        this.members = members;
        this.distributedStore = distributedStore;
    }

    /**
     * Get the {@code key} represented by this resource.
     * <p/>
     * To be used <strong>for testing only!</strong>
     */
    String getKey() {
        return key;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public KeyValue get() throws Exception {
        LOGGER.info("get: {}", key);

        try {
            ListenableFuture<KeyValue> getFuture = distributedStore.get(key);
            return getFuture.get(ResourceConstants.COMMAND_TIMEOUT, ResourceConstants.COMMAND_TIMEOUT_TIME_UNIT);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof NotLeaderException) {
                throw createCannotSubmitCommandException((NotLeaderException) cause);
            } else if (cause instanceof KeyNotFoundException) {
                throw (KeyNotFoundException) cause;
            } else {
                throw e;
            }
        }
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public @Nullable KeyValue update(SetValue setValue) throws Exception {
        if (!setValue.hasNewValue() && !setValue.hasExpectedValue()) {
            throw new IllegalArgumentException(String.format("key:%s - bad request: expectedValue and newValue not set", key));
        }

        if (setValue.hasExpectedValue()) {
            return compareAndSet(setValue);
        } else {
            return set(setValue);
        }
    }

    private KeyValue set(SetValue setValue) throws Exception {
        LOGGER.info("set: {}->{}", key, setValue.getNewValue());

        checkArgument(setValue.getNewValue() != null, "key:%s - null newValue: setValue:%s", key, setValue);

        try {
            ListenableFuture<KeyValue> setFuture = distributedStore.set(key, setValue);
            return setFuture.get(ResourceConstants.COMMAND_TIMEOUT, ResourceConstants.COMMAND_TIMEOUT_TIME_UNIT);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof NotLeaderException) {
                throw createCannotSubmitCommandException((NotLeaderException) cause);
            } else {
                throw e;
            }
        }
    }

    private KeyValue compareAndSet(SetValue setValue) throws Exception {
        LOGGER.info("cas: {}->{} if {}", key, setValue.getNewValue(), setValue.getExpectedValue());

        try {
            ListenableFuture<KeyValue> casFuture = distributedStore.compareAndSet(key, setValue);
            return casFuture.get(ResourceConstants.COMMAND_TIMEOUT, ResourceConstants.COMMAND_TIMEOUT_TIME_UNIT);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof NotLeaderException) {
                throw createCannotSubmitCommandException((NotLeaderException) cause);
            } else if (cause instanceof KayVeeException) {
                throw (KayVeeException) cause;
            } else {
                throw e;
            }
        }
    }

    @DELETE
    public void delete() throws Exception {
        LOGGER.info("delete: {}", key);

        try {
            ListenableFuture<Void> deleteFuture = distributedStore.delete(key);
            deleteFuture.get(ResourceConstants.COMMAND_TIMEOUT, ResourceConstants.COMMAND_TIMEOUT_TIME_UNIT);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof NotLeaderException) {
                throw createCannotSubmitCommandException((NotLeaderException) cause);
            } else {
                throw e;
            }
        }
    }

    private CannotSubmitCommandException createCannotSubmitCommandException(NotLeaderException cause) {
        return new CannotSubmitCommandException(cause, members);
    }
}
