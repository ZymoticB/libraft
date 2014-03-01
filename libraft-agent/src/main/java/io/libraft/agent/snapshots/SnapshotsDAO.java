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

package io.libraft.agent.snapshots;

import com.google.common.base.Objects;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

// FIXME (AG): I'm essentially using the timestamp as a primary key
abstract class SnapshotsDAO {

    @Transaction
    void createSnapshotsTableWithIndex() {
        createSnapshotsTable();
        createTimestampIndexForSnapshotsTable();
    }

    @SqlUpdate("create table if not exists snapshots(filename varchar(255) not null, ts bigint unique, last_term bigint not null, last_index bigint not null)")
    abstract void createSnapshotsTable();

    @SqlUpdate("create index if not exists ts_index on snapshots(ts)")
    abstract void createTimestampIndexForSnapshotsTable();

    @SqlUpdate("insert into snapshots(filename, ts, last_term, last_index) values(:m.filename, :m.timestamp, :m.lastTerm, :m.lastIndex)")
    abstract void addSnapshot(@BindBean("m") SnapshotMetadata metadata);

    @SqlUpdate("delete from snapshots where ts=:snapshotTimestamp")
    abstract void removeSnapshotWithTimestamp(@Bind("snapshotTimestamp") long snapshotTimestamp);

    @SqlQuery("select filename, ts, last_term, last_index from snapshots where ts in (select max(ts) from snapshots)")
    @Mapper(SnapshotMetadataResultMapper.class)
    abstract @Nullable SnapshotMetadata getLatestSnapshot();

    @SqlQuery("select count(*) from snapshots")
    abstract int getNumSnapshots();

    @SqlQuery("select filename, ts, last_term, last_index from snapshots order by ts desc")
    @Mapper(SnapshotMetadataResultMapper.class)
    abstract ResultIterator<SnapshotMetadata> getAllSnapshotsFromLatestToOldest();

    /**
     * Closes the connection used by the DAO.
     */
    @SuppressWarnings("unused")
    abstract void close();

    //-------------------------------------------------------------------------
    //
    // types and mappers
    //

    // needs to be public for access by JDBI
    public static final class SnapshotMetadataResultMapper implements ResultSetMapper<SnapshotMetadata> {

        @Override
        public SnapshotMetadata map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            if (!r.isAfterLast()) {
                return new SnapshotMetadata(r.getString("filename"), r.getLong("ts"), r.getLong("last_term"), r.getLong("last_index"));
            } else {
                return null;
            }
        }
    }

    // needs to be public for access by JDBI
    public static final class SnapshotMetadata {

        private final String filename;
        private final long timestamp;
        private final long lastTerm;
        private final long lastIndex;

        SnapshotMetadata(String filename, long timestamp, long lastTerm, long lastIndex) {
            this.filename = filename;
            this.timestamp = timestamp;
            this.lastTerm = lastTerm;
            this.lastIndex = lastIndex;
        }

        public String getFilename() {
            return filename;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getLastTerm() {
            return lastTerm;
        }

        public long getLastIndex() {
            return lastIndex;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SnapshotMetadata other = (SnapshotMetadata) o;

            return filename.equals(other.filename) && timestamp == other.timestamp && lastTerm == other.lastTerm && lastIndex == other.lastIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(filename, timestamp, lastTerm, lastIndex);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add("filename", filename)
                    .add("timestamp", timestamp)
                    .add("lastTerm", lastTerm)
                    .add("lastIndex", lastIndex)
                    .toString();
        }
    }
}
