package io.github.javalyhn.danmakuflow.tools;

import io.github.javalyhn.danmakuflow.protocol.ClientCommand;
import tools.jackson.databind.json.JsonMapper;

/** Converts one console line to a validated client envelope. */
final class CommandParser {
    private final JsonMapper mapper = JsonMapper.builder().build();

    record Action(String json, boolean quit) {}

    Action parse(String line) {
        if (line.equals("/quit")) return new Action(null, true);
        var root = mapper.createObjectNode();
        if (line.equals("/like") || line.startsWith("/like ") || line.startsWith("/like\t")) {
            String[] parts = line.trim().split("\\s+");
            if (parts.length > 2) throw new IllegalArgumentException("Usage: /like [positive count]");
            long count = 1;
            if (parts.length == 2) {
                try { count = Long.parseLong(parts[1]); }
                catch (NumberFormatException invalid) {
                    throw new IllegalArgumentException("Like count must be a positive signed 64-bit integer");
                }
                if (count <= 0) throw new IllegalArgumentException("Like count must be positive");
            }
            var like = new ClientCommand.Like(count);
            root.put("type", 103).set("data", mapper.createObjectNode().put("count", like.count()));
        } else {
            var chat = new ClientCommand.Chat(line);
            root.put("type", 101).set("data", mapper.createObjectNode().put("content", chat.content()));
        }
        return new Action(mapper.writeValueAsString(root), false);
    }
}
