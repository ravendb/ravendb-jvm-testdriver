package net.ravendb.test.driver;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class FailureCachingLazyTest {

    @Test
    public void valueIsCreatedOnce() {
        AtomicInteger calls = new AtomicInteger();
        FailureCachingLazy<Object> lazy = new FailureCachingLazy<>(() -> {
            calls.incrementAndGet();
            return new Object();
        });

        assertThat(lazy.isValueCreated()).isFalse();

        Object first = lazy.getValue();

        assertThat(lazy.getValue()).isSameAs(first);
        assertThat(lazy.isValueCreated()).isTrue();
        assertThat(calls).hasValue(1);
    }

    @Test
    public void failureIsCachedAndTheOriginalExceptionRethrown() {
        AtomicInteger calls = new AtomicInteger();
        FailureCachingLazy<Object> lazy = new FailureCachingLazy<>(() -> {
            calls.incrementAndGet();
            throw new IllegalStateException("boom");
        });

        Throwable first = catchThrowable(lazy::getValue);
        Throwable second = catchThrowable(lazy::getValue);

        assertThat(first)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
        assertThat(second).isSameAs(first);
        assertThat(calls)
                .as("a failed factory must not be retried")
                .hasValue(1);
    }

    @Test
    public void valueIsNotCreatedAfterAFailure() {
        FailureCachingLazy<Object> lazy = new FailureCachingLazy<>(() -> {
            throw new IllegalStateException("boom");
        });

        catchThrowable(lazy::getValue);

        assertThat(lazy.isValueCreated()).isFalse();
    }
}
