package io.github.javalyhn.danmakuflow.protocol;

public sealed interface ClientCommand permits ClientCommand.Chat, ClientCommand.Like {
    record Chat(String content) implements ClientCommand {
        public Chat { Validation.content(content); }
    }

    /**
     * Zero means one like, preserving Go defaults. Negative and overflowing JSON counts
     * are rejected intentionally: Redis INCRBY accepts only signed 64-bit increments.
     */
    record Like(long count) implements ClientCommand {
        public Like {
            if (count < 0) throw new ProtocolException("Like count must be nonnegative");
            if (count == 0) count = 1;
        }
    }
}
