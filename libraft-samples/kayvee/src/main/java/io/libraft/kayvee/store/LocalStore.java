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

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import io.libraft.kayvee.api.KeyValue;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents the local server's view of the replicated key-value store.
 * Methods in the class are called to transform the local server's key-value
 * state whenever a {@link KayVeeCommand} is committed to the cluster.
 * <p/>
 * This state is not persistent, and will have to be refreshed on every
 * restart. As a result, this component is best thought of as a cache that
 * contains the applied cluster state to a given point in time.
 * <p/>
 * This component is thread-safe.
 */
public class LocalStore {

    private final Map<String, String> entries = Maps.newHashMap();

    private long lastAppliedIndex = 0;

    //
    // store operations
    //

    /**
     * Get the log index of the last applied command.
     * The index returned will monotonically increase within the
     * lifetime of the process.
     *
     * @return index >=0 of the last applied command
     */
    long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    //
    // IMPORTANT: always update lastAppliedIndex first for the operations below
    //

    /**
     * A noop operation. This call does not affect the
     * key-value state, but does update the last applied command index.
     *
     * @param index log index >= 0 associated with this command
     */
    synchronized void nop(final long index) {
        updateLastAppliedIndex(index);
    }

    /**
     * Get the value for a key.
     *
     * @param index log index >= 0 associated with this command
     * @param key non-null (possibly empty) key for which the value should be retrieved
     * @return a {@code KeyValue} instance containing the most up-to-date {@code key=>value} pair for {@code key}
     * @throws KayVeeException if {@code key} does not exist
     */
    synchronized KeyValue get(final long index, final String key) throws KayVeeException {
        updateLastAppliedIndex(index);

        String value = entries.get(key);

        if (value == null) {
            throw new KeyNotFoundException(key);
        }

        return new KeyValue(key, value);
    }

    /**
     * Get all (key, value) pairs.
     *
     * @param index log index >= 0 associated with this command
     * @return a <strong>copy</strong> of the most up-to-date {@code key=>value} pairs for all keys
     */
    synchronized Collection<KeyValue> getAll(final long index) {
        updateLastAppliedIndex(index);

        Collection<KeyValue> copiedEntries = Lists.newArrayListWithCapacity(entries.size());
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            copiedEntries.add(new KeyValue(entry.getKey(), entry.getValue()));
        }

        return copiedEntries;
    }

    /**
     * Set a key to a value.
     *
     * @param index log index >= 0 associated with this command
     * @param key non-null (possibly empty) key for which the value should be set
     * @param value non-null, non-empty value for this key
     * @return a {@code KeyValue} instance containing the most up-to-date {@code key=>value} pair for {@code key}
     */
    synchronized KeyValue set(final long index, final String key, final String value) {
        updateLastAppliedIndex(index);

        entries.put(key, value);

        return new KeyValue(key, entries.get(key));
    }

    /**
     * Do a compare-and-set (CAS), aka. test-and-set, operation for key.
     *
     * @param index log index >= 0 associated with this command
     * @param key non-null (possibly empty) key for which the value should be set
     * @param expectedValue existing value associated with {@code key}.
     *                      If {@code expectedValue} is null {@code LocalStore} <strong>should not</strong>
     *                      contain a {@code key=>value} pair for {@code key}
     * @param newValue new value to be associated with {@code key}.
     *                 If {@code newValue} is null the existing {@code key=>value} pair is deleted
     * @return a {@code KeyValue} instance containing the most up-to-date {@code key=>value} pair for {@code key},
     * or {@code null} if this operation removed the {@code key=>value} pair
     * @throws KeyNotFoundException if {@code key} does not exist
     * @throws ValueMismatchException if {@code expectedValue} does not match the <strong>current</strong> value for {@code key}
     * @throws KeyAlreadyExistsException if {@code expectedValue} is null (indicating that
     * a new key-value mapping should be created), but a {@code key=>value} pair already exists
     */
    synchronized @Nullable KeyValue compareAndSet(
            final long index,
            final String key,
            final @Nullable String expectedValue,
            final @Nullable String newValue)
            throws KeyNotFoundException, ValueMismatchException, KeyAlreadyExistsException {
        if (expectedValue == null && newValue == null) {
            // while I _could_ update the lastAppliedIndex here, calling code should never call us with these arguments
            throw new IllegalArgumentException(String.format("both expectedValue and newValue null for %s (index:%s)", key, index));
        }

        updateLastAppliedIndex(index);

        String existingValue = entries.get(key);

        // all possibilities
        // -------------------------------------------------------------------
        // | existingValue | expectedValue |    result
        // |     null      |     null      |    assert newValue != null; create key=>newValue
        // |     null      |     !null     |    KeyNotFoundException
        // |     !null     |     null      |    KeyAlreadyExistsException
        // |     !null     |     !null     |    expectedValue != existingValue ? ValueMismatchException : ( newValue != null ? update key=>newValue : delete key)
        //
        // existingValue != null && expectedValue != null (from last row above)
        // --------------------------------------------------------------------
        // |     match     | result
        // |       Y       | newValue != null ? update key=>newValue : delete key)
        // |       N       | ValueMismatchException

        if (existingValue == null) {
            if (expectedValue == null) {
                entries.put(key, newValue);
            } else {
                throw new KeyNotFoundException(key);
            }
        } else {
            if (expectedValue != null) {
                if (existingValue.equals(expectedValue)) {
                    if (newValue != null) {
                        entries.put(key, newValue);
                    } else {
                        entries.remove(key);
                    }
                } else {
                    throw new ValueMismatchException(key, expectedValue, existingValue);
                }
            } else {
                throw new KeyAlreadyExistsException(key);
            }
        }

        // TODO (AG): I could just look at newValue, but for now I'll look at the map again
        String finalValue = entries.get(key);
        return finalValue != null ? new KeyValue(key, finalValue) : null;
    }

    /**
     * Delete the value for a key. This operation is a noop if the key does not exist.
     *
     * @param index log index >= 0 associated with this command
     * @param key non-null (possibly empty) key for which the value should be deleted
     */
    synchronized void delete(final long index, final String key) {
        updateLastAppliedIndex(index);

        entries.remove(key);
    }

    private void updateLastAppliedIndex(long index) {
        checkArgument(index > 0, "index must be positive: given:%s", index);
        checkArgument(index > lastAppliedIndex, "index must be monotonic: given:%s", index);

        lastAppliedIndex = index;
    }

    //
    // state {de}serialization
    // see: http://jackson-users.ning.com/forum/topics/appending-pojos-to-a-json-file
    //

    synchronized long dumpSnapshotTo(OutputStream snapshotOutputStream) throws IOException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                mapper.writeValue(snapshotOutputStream, new KeyValue(entry.getKey(), entry.getValue()));
            }

            return lastAppliedIndex;
        } finally {
            Closeables.close(snapshotOutputStream, true);
        }
    }

    synchronized void loadSnapshotFrom(long index, InputStream snapshotInputStream) throws IOException {
        try {
            entries.clear();

            ObjectMapper mapper = new ObjectMapper();
            ObjectReader reader = mapper.reader(KeyValue.class);
            MappingIterator<KeyValue> it = reader.readValues(snapshotInputStream);
            while(it.hasNext()) {
                KeyValue keyValue = it.next();
                entries.put(keyValue.getKey(), keyValue.getValue());
            }

            lastAppliedIndex = index;
        } finally {
            Closeables.close(snapshotInputStream, true);
        }
    }

    //
    // the following commands are to be used within unit tests only
    // they set the underlying state, do not update lastAppliedIndex and do not perform any verification
    //

    /**
     * Set the log index associated with the last command {@code LocalStore} applied.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @param index log index >= 0 for the last command applied
     */
    void setLastAppliedIndexForUnitTestsOnly(long index) {
        updateLastAppliedIndex(index);
    }

    /**
     * Set the value for a key.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @param key non-null (possibly empty) key for which the value should be set
     * @param value non-null, non-empty value for this key
     */
    void setKeyValueForUnitTestsOnly(String key, String value) {
        entries.put(key, value);
    }

    /**
     * Get the value for a key.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @param key non-null (possibly empty) key for which the value should be retrieved
     * @return current value associated with this key, or null if the key does not exist
     */
    @Nullable String getKeyValueForUnitTestsOnly(String key) {
        return entries.get(key);
    }

    Collection<KeyValue> getAllForUnitTestsOnly() {
        List<KeyValue> copiedEntries = Lists.newArrayListWithCapacity(entries.size());

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            copiedEntries.add(new KeyValue(entry.getKey(), entry.getValue()));
        }

        return copiedEntries;
    }
}
