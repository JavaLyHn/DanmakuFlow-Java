package io.github.javalyhn.danmakuflow.tools;

import java.net.URI;

record ClientOptions(URI uri, boolean help) {
    private static final String DEFAULT_URL = "ws://127.0.0.1:8080/ws?uid=1&name=java-cli&room=test";

    static ClientOptions parse(String[] args) {
        if (args.length == 1 && (args[0].equals("--help") || args[0].equals("-h")))
            return new ClientOptions(null, true);
        String url;
        if (args.length == 0) url = DEFAULT_URL;
        else if (args.length == 1 && !args[0].startsWith("-")) url = args[0];
        else if (args.length == 2 && args[0].equals("--url")) url = args[1];
        else throw new IllegalArgumentException("Usage: java -jar danmaku-tools.jar [--url] <ws/wss URL>; --help for commands");
        try {
            URI uri = URI.create(url);
            if (!("ws".equalsIgnoreCase(uri.getScheme()) || "wss".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null || uri.getUserInfo() != null || uri.getFragment() != null
                    || uri.getPort() > 65535 || uri.getPort() == 0)
                throw new IllegalArgumentException();
            return new ClientOptions(uri, false);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Invalid WebSocket URL: use ws/wss, a host, and no user-info credentials or fragment");
        }
    }
}
