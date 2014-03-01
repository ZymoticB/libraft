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

package io.libraft.agent.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.libraft.algorithm.RaftConstants;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
@SuppressWarnings("unused")
public final class RaftSnapshotsConfiguration {

    private static final String DEFAULT_SNAPSHOTS_DIRECTORY = "."; // cwd

    private static final String MIN_ENTRIES_TO_SNAPSHOT = "minEntriesToSnapshot";
    private static final String SNAPSHOT_CHECK_INTERVAL = "snapshotCheckInterval";
    private static final String SNAPSHOTS_DIRECTORY = "snapshotsDirectory";

    @Min(RaftConstants.SNAPSHOTS_DISABLED)
    @Max(Integer.MAX_VALUE) // I can't say "infinite", so I'll use this instead
    @NotNull
    @JsonProperty(MIN_ENTRIES_TO_SNAPSHOT)
    private int minEntriesToSnapshot = RaftConstants.SNAPSHOTS_DISABLED;

    @Min(RaftConfigurationConstants.ONE_SECOND)
    @Max(RaftConfigurationConstants.TWELVE_HOURS)
    @JsonProperty(SNAPSHOT_CHECK_INTERVAL)
    private long snapshotCheckInterval = RaftConstants.SNAPSHOT_CHECK_INTERVAL;

    @NotEmpty
    @JsonProperty(SNAPSHOTS_DIRECTORY)
    private String snapshotsDirectory = DEFAULT_SNAPSHOTS_DIRECTORY;

    public int getMinEntriesToSnapshot() {
        return minEntriesToSnapshot;
    }

    public void setMinEntriesToSnapshot(int minEntriesToSnapshot) {
        this.minEntriesToSnapshot = minEntriesToSnapshot;
    }

    public long getSnapshotCheckInterval() {
        return snapshotCheckInterval;
    }

    public void setSnapshotCheckInterval(long snapshotCheckInterval) {
        this.snapshotCheckInterval = snapshotCheckInterval;
    }

    public String getSnapshotsDirectory() {
        return snapshotsDirectory;
    }

    public void setSnapshotsDirectory(String snapshotsDirectory) {
        this.snapshotsDirectory = snapshotsDirectory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftSnapshotsConfiguration other = (RaftSnapshotsConfiguration) o;
        return minEntriesToSnapshot == other.minEntriesToSnapshot
                && snapshotCheckInterval == other.snapshotCheckInterval
                && snapshotsDirectory.equals(other.snapshotsDirectory);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(minEntriesToSnapshot, snapshotCheckInterval, snapshotsDirectory);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(MIN_ENTRIES_TO_SNAPSHOT, minEntriesToSnapshot)
                .add(SNAPSHOT_CHECK_INTERVAL, snapshotCheckInterval)
                .add(SNAPSHOTS_DIRECTORY, snapshotsDirectory)
                .toString();
    }
}
