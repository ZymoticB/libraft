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

import io.libraft.kayvee.api.KeyValue;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public final class LocalStoreTest {
    private static final String KEY = "leslie";
    private static final String EXPECTED_VALUE = "lamport@microsoft";
    private static final String NEW_VALUE = "lamport";

    private LocalStore localStore;

    @Before
    public void setup() {
        localStore = new LocalStore();
    }

    @Test
    public void shouldReturn0AsLastAppliedIndexAfterInitialization() throws Exception {
        long lastAppliedIndex = localStore.getLastAppliedIndex();
        assertThat(lastAppliedIndex, equalTo(0L));
    }

    @Test
    public void shouldReturnCorrectLastAppliedIndex() throws Exception {
        final long index = 31;

        localStore.setLastAppliedIndexForUnitTestsOnly(index);

        long lastAppliedIndex = localStore.getLastAppliedIndex();
        assertThat(lastAppliedIndex, equalTo(index));
    }

    @Test
    public void shouldUpdateIndexForANopCommand() throws Exception {
        final long originalIndex = 123;

        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        localStore.nop(originalIndex + 1);

        long updatedIndex = localStore.getLastAppliedIndex();
        assertThat(updatedIndex, equalTo(originalIndex + 1));
    }

    @Test
    public void shouldThrowKeyNotFoundExceptionIfKeyDoesNotExist()  {
        final int newIndex = 1;
        KayVeeException thrownException = null;
        try {
            localStore.get(newIndex, "FAKE_KEY");
        } catch (KayVeeException e) {
            thrownException = e;
        }
        thrownException = checkNotNull(thrownException);

        assertThat(thrownException, instanceOf(KeyNotFoundException.class));
        assertThat(((KeyNotFoundException) thrownException).getKey(), equalTo("FAKE_KEY"));

        assertThatLocalStoreHasKeyValue("FAKE_KEY", null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldReturnCorrectValueIfKeyExists() throws Exception {
        // set the key
        localStore.setKeyValueForUnitTestsOnly(KEY, NEW_VALUE);

        // get the key/value
        final int newIndex = 27;
        KeyValue keyValue = localStore.get(newIndex, KEY);

        assertThat(keyValue, notNullValue());
        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        assertThatLocalStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldReturnEmptyCollectionIfGetAllIsCalledAndThereAreNoEntriesInDB() throws Exception {
        final long index = 81;
        Collection<KeyValue> all = localStore.getAll(index);

        all = checkNotNull(all);
        assertThat(all, hasSize(0));

        assertThatLastAppliedIndexHasValue(index);
    }

    @Test
    public void shouldReturnCollectionWithAllKeyValuesInItWhenGetAllIsCalled() throws Exception {
        final KeyValue[] keyValues = {
                new KeyValue("barbara", "liskov"),
                new KeyValue("fred", "schneider"),
                new KeyValue("ken", "birman"),
                new KeyValue("leslie", "lamport"),
                new KeyValue("nancy", "lynch")
        };

        for (KeyValue keyValue : keyValues) {
            localStore.setKeyValueForUnitTestsOnly(keyValue.getKey(), keyValue.getValue());

        }

        final long index = 77;
        Collection<KeyValue> all = localStore.getAll(index);

        all = checkNotNull(all);
        assertThat(all, containsInAnyOrder(keyValues));

        assertThatLastAppliedIndexHasValue(index);
    }

    @Test
    public void shouldCreateKeyIfItDoesNotExistWhenSetIsCalled() {
        final long index = 172;
        localStore.set(index, KEY, EXPECTED_VALUE);

        assertThatLocalStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedIndexHasValue(index);
    }

    @Test
    public void shouldUpdateKeyIfItExistsWhenSetIsCalled() {
        // set the original value of the key
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);
        assertThatLocalStoreHasKeyValue(KEY, EXPECTED_VALUE);

        // now, update to the new value
        final long index = 99;
        localStore.set(index, KEY, NEW_VALUE);

        assertThatLocalStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedIndexHasValue(index);
    }

    @Test
    public void shouldThrowIfGivenIndexIsZero() {
        final long originalIndex = 17;

        // set the original index
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // now, try to set a index with '0'
        IllegalArgumentException setException = null;
        try {
            localStore.set(0, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(originalIndex);
    }

    @Test
    public void shouldThrowIfGivenIndexIsNegative() {
        final long originalIndex = 27;

        // set the original index
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // now, try to set a negative index
        IllegalArgumentException setException = null;
        try {
            localStore.set(-17, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(originalIndex);
    }

    @Test
    public void shouldThrowIfGivenIndexIsNotMonotonicallyIncreasing() {
        final long originalIndex = 37;

        // set the original index
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // now, try to set a index with one less than the original command index
        IllegalArgumentException setException = null;
        try {
            localStore.set(36, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(originalIndex);
    }

    @Test
    public void shouldCreateKeyIfCASIsAttemptedForKeyThatDoesNotExist() throws Exception {
        // do the CAS
        final long newIndex = 66;
        KeyValue keyValue = localStore.compareAndSet(newIndex, KEY, null, NEW_VALUE);
        keyValue = checkNotNull(keyValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        // check that the store and command index were updated
        assertThatLocalStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldUpdateKeyIfCASIsAttemptedForKeyWhoseNonNullExpectedValueMatchesCurrentValue() throws Exception {
        // set the expected value
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // do the CAS
        final long newIndex = 71;
        KeyValue keyValue = localStore.compareAndSet(newIndex, KEY, EXPECTED_VALUE, NEW_VALUE);
        keyValue = checkNotNull(keyValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        assertThatLocalStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldDeleteKeyIfCASIsAttemptedForKeyWhoseNonNullExpectedValueMatchesCurrentValueAndNewValueIsNull() throws Exception {
        // set the expected value
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // do the CAS
        final long newIndex = 23;
        KeyValue keyValue = localStore.compareAndSet(newIndex, KEY, EXPECTED_VALUE, null);
        assertThat(keyValue, nullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfBothExpectedValueAndNewValueAreNull() throws Exception {
        // set the lastAppliedIndex to some value
        final long preCASLastAppliedIndex = 276;
        localStore.setLastAppliedIndexForUnitTestsOnly(preCASLastAppliedIndex);

        // do the cas
        final long casCommandIndex = preCASLastAppliedIndex + 1;
        Exception casException = null;
        try {
            localStore.compareAndSet(casCommandIndex, KEY, null, null);
        } catch (IllegalArgumentException e) {
            casException = e;
        }

        // neither the value nor the command index should have been updated
        assertThat(casException, notNullValue());
        assertThatLastAppliedIndexHasValue(preCASLastAppliedIndex);
    }

    @Test
    public void shouldThrowKeyAlreadyExistsExceptionIfExpectedValueIsNullButCurrentValueIsNotNull() throws Exception {
        // set the expected value
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // do the CAS
        final long newIndex = 77;
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newIndex, KEY, null, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        assertThat(casException, instanceOf(KeyAlreadyExistsException.class));
        assertThat(((KeyAlreadyExistsException) casException).getKey(), equalTo(KEY));

        assertThatLocalStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldThrowValueMismatchExceptionIfExpectedValueIsNotNullButDoesNotMatchCurrentValue() throws Exception {
        // set the expected value
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // do the CAS
        final long newIndex = 71129837;
        final String mismatchedExpectedValue = "lamport@ibm";
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newIndex, KEY, mismatchedExpectedValue, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        ValueMismatchException valueMismatchException = (ValueMismatchException) casException;
        assertThat(valueMismatchException.getKey(), equalTo(KEY));
        assertThat(valueMismatchException.getExpectedValue(), equalTo(mismatchedExpectedValue));
        assertThat(valueMismatchException.getExistingValue(), equalTo(EXPECTED_VALUE));

        assertThatLocalStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldThrowKeyNotFoundExceptionIfExpectedValueIsNotNullButKeyDoesNotExist() throws Exception {
        final long newIndex = 372;
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newIndex, KEY, EXPECTED_VALUE, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        KeyNotFoundException keyNotFoundException = (KeyNotFoundException) casException;
        assertThat(keyNotFoundException.getKey(), equalTo(KEY));

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldDeleteKeyIfItExists() throws Exception {
        // set the initial value for the key
        localStore.set(1, KEY, EXPECTED_VALUE);

        // delete it
        final long newIndex = 37;
        localStore.delete(newIndex, KEY);

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldNoopIfDeleteCalledForKeyAndItDoesNotExist() throws Exception {
        final long newIndex = 37;
        localStore.delete(newIndex, KEY);

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldSerializeKeyValuesToAndDeserializeKeyValuesFromStream() throws IOException {
        // set the original command index
        long originalIndex = 17;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // store all the initial key=>value pairs
        KeyValue[] keyValues = new KeyValue[] {
                new KeyValue("1", "ONE"),
                new KeyValue("2", "TWO"),
                new KeyValue("3", "THREE"),
                new KeyValue("4", "FOUR")
        };

        for (KeyValue keyValue : keyValues) {
            localStore.setKeyValueForUnitTestsOnly(keyValue.getKey(), keyValue.getValue());
        }

        // serialize
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long index = localStore.dumpSnapshotTo(out);
        assertThat(index, equalTo(originalIndex));

        // now, change the command index and add another key=>value pair (i.e. change localStore state)
        localStore.setLastAppliedIndexForUnitTestsOnly(18);
        assertThat(localStore.getLastAppliedIndex(), equalTo(18L));
        localStore.setKeyValueForUnitTestsOnly("5", "FIVE");

        // let's read the serialized data
        localStore.loadSnapshotFrom(index, new ByteArrayInputStream(out.toByteArray()));

        // check that the final state is equal to the initial state
        assertThat(localStore.getLastAppliedIndex(), equalTo(originalIndex));
        assertThat(localStore.getAllForUnitTestsOnly(), containsInAnyOrder(keyValues));
    }

    @Test
    public void shouldSerializeEmptyStateToAndFromStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long index = localStore.dumpSnapshotTo(out);
        assertThat(index, equalTo(0L));

        localStore.loadSnapshotFrom(index, new ByteArrayInputStream(out.toByteArray()));
        assertThat(localStore.getAllForUnitTestsOnly(), Matchers.<KeyValue>iterableWithSize(0));
    }

    private void assertThatLastAppliedIndexHasValue(final long expectedLastAppliedIndex) {
        assertThat(localStore.getLastAppliedIndex(), equalTo(expectedLastAppliedIndex));
    }

    private void assertThatLocalStoreHasKeyValue(final String key, @Nullable final String value) {
        assertThat(localStore.getKeyValueForUnitTestsOnly(key), equalTo(value));
    }
}
