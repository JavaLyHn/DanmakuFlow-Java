package io.github.javalyhn.danmakuflow.persistence;

import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Coordination only: the owner remains responsible for transaction and client cleanup. */
final class ShutdownWaiter {
    private ShutdownWaiter() {}
    static boolean await(CountDownLatch finished, Duration timeout, PrintStream diagnostics) {
        long started = System.nanoTime();
        long budget = timeout.toNanos();
        boolean interrupted = false;
        try {
            while (finished.getCount() != 0) {
                long remaining = budget - (System.nanoTime() - started);
                if (remaining <= 0) {
                    diagnostics.println("Persistence shutdown deadline expired; owner cleanup remains unfinished; "
                            + "in-flight transaction may be replayed. Shutdown acknowledges no additional offsets.");
                    return false;
                }
                try { if (finished.await(remaining, TimeUnit.NANOSECONDS)) return true; }
                catch (InterruptedException e) { interrupted = true; }
            }
            return true;
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
    }
}
