package io.github.javalyhn.danmakuflow.persistence;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class ShutdownWaiterTest {
    // An unfinished owner and repeated interrupts cannot reset or remove the overall deadline.
    @Test void unfinishedOwnerTimesOutVisiblyDespiteRepeatedInterrupts() throws Exception {
        var unfinished = new CountDownLatch(1);
        var output = new ByteArrayOutputStream();
        var interrupted = new AtomicBoolean();
        var completed = new AtomicBoolean(true);
        var entered = new CountDownLatch(1);
        var waiter = new Thread(() -> {
            entered.countDown();
            completed.set(await(unfinished, Duration.ofMillis(80), new PrintStream(output)));
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        waiter.setDaemon(true); waiter.start(); entered.await();
        long start = System.nanoTime();
        try {
            while (waiter.isAlive() && System.nanoTime() - start < Duration.ofMillis(500).toNanos()) {
                waiter.interrupt(); waiter.join(5);
            }
            assertFalse(waiter.isAlive(), "shutdown wait must terminate while owner remains unfinished");
            assertFalse(completed.get()); assertTrue(interrupted.get()); assertEquals(1, unfinished.getCount());
            assertTrue(output.toString().contains("deadline expired"), "expiry must be visible");
        } finally { unfinished.countDown(); waiter.join(1000); }
    }

    @Test void completedOwnerDoesNotReportTimeout() {
        var output = new ByteArrayOutputStream();
        assertTrue(await(new CountDownLatch(0), Duration.ofMillis(80), new PrintStream(output)));
        assertEquals("", output.toString());
    }

    private boolean await(CountDownLatch owner, Duration timeout, PrintStream output) {
        return ShutdownWaiter.await(owner, timeout, output);
    }
}
