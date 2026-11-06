package io.github.javalyhn.danmakuflow.gateway;
import io.github.javalyhn.danmakuflow.protocol.RoomStats;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
public interface RoomStatisticsStore {
 CompletionStage<Void> addLikes(String roomId, long count);
 CompletionStage<RoomStats> refresh(String roomId, String instanceId, int online, Duration ttl);
 CompletionStage<Void> remove(String roomId, String instanceId);
}
