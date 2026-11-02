package net.ravendb.test.driver;

import java.util.function.Supplier;

/**
 * A lazy value whose factory runs at most once: a failure is cached and rethrown on every later call.
 * <p>
 * {@link net.ravendb.client.documents.Lazy} retries after a failure, which for a test server means every
 * test would relaunch it - re-preparing the test arguments in the same options and hiding the root cause
 * behind 'The server was already started'. This is the contract of C#'s
 * {@code Lazy<T>(..., LazyThreadSafetyMode.ExecutionAndPublication)}, which caches the exception.
 */
final class FailureCachingLazy<T> {

    private final Supplier<T> valueFactory;

    private volatile boolean evaluated;
    private T value;
    private RuntimeException failure;

    FailureCachingLazy(Supplier<T> valueFactory) {
        this.valueFactory = valueFactory;
    }

    /**
     * Whether the factory has run and produced a value - false after a failure, like C#'s {@code IsValueCreated}.
     */
    boolean isValueCreated() {
        return evaluated && failure == null;
    }

    T getValue() {
        if (!evaluated) {
            synchronized (this) {
                if (!evaluated) {
                    try {
                        value = valueFactory.get();
                    } catch (RuntimeException e) {
                        failure = e;
                    }
                    evaluated = true;
                }
            }
        }

        if (failure != null) {
            throw failure;
        }

        return value;
    }
}
