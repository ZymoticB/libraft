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

package io.libraft;

/**
 * Implemented by a class that contains state committed to the Raft cluster.
 * <p/>
 * A {@code Committed} can be <strong>one of</strong> these two types:
 * <ul>
 *     <li>{@code LOGENTRY}: an individual entry in the Raft cluster log.</li>
 *     <li>{@code SNAPSHOT}: a snapshot representing the aggregate committed state
 *         of the Raft cluster log at a certain log index.</li>
 * </ul>
 * It is the responsibility of implementing classes to define which type
 * they are and provide methods for the caller to load type-specific data.
 */
public interface Committed {

    /**
     * The type of this committed state.
     */
    enum Type {

        /**
         * An individual entry in the Raft cluster log.
         */
        LOGENTRY,

        /**
         * A snapshot representing the aggregate committed state
         * of the Raft cluster log at a certain log index.
         */
        SNAPSHOT,
    }

    /**
     * Get the {@code Type} of this committed state.
     *
     * @return the type of this committed state
     */
    Type getType();
}
