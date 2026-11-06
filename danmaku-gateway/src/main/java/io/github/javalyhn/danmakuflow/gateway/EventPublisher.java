package io.github.javalyhn.danmakuflow.gateway;
import io.github.javalyhn.danmakuflow.protocol.*;
import java.util.concurrent.CompletionStage;
/** Production completion means broker ACK, not merely enqueueing a record. */
public interface EventPublisher { CompletionStage<Void> publish(DanmakuEvent event); }
