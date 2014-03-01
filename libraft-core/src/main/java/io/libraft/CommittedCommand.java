package io.libraft;

/**
 * Implemented by classes that represent a committed {@link Command}.
 */
public interface CommittedCommand extends Committed {

    /**
     * Get the log index of the committed {@code Command}.
     *
     * @return index > 0 in the Raft log of the committed {@code Command}
     */
    long getIndex();

    /**
     * Get the committed {@code Command}.
     *
     * @return the committed {@code Command} instance
     */
    Command getCommand();
}