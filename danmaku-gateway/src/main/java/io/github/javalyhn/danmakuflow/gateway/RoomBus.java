package io.github.javalyhn.danmakuflow.gateway;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
/** One listener per local room. Subscribe completion confirms readiness; unsubscribe
 * completion confirms that the old subscription is detached. Adapters must bound their own buffers. */
public interface RoomBus {
 CompletionStage<Void> subscribe(String roomId, Consumer<String> listener);
 CompletionStage<Void> unsubscribe(String roomId);
 CompletionStage<Void> publish(String roomId, String payload);
}
