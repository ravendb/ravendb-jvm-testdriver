package net.ravendb.test.driver;

import net.ravendb.client.documents.Lazy;

import java.time.Duration;

@SuppressWarnings({"WeakerAccess", "unused"})
public class GetDocumentStoreOptions {
    private static final Lazy<GetDocumentStoreOptions> DEFAULT = new Lazy<>(() -> new GetDocumentStoreOptions());

    static GetDocumentStoreOptions getDefault() {
        return DEFAULT.getValue();
    }

    private Duration waitForIndexingTimeout;

    public Duration getWaitForIndexingTimeout() {
        return waitForIndexingTimeout;
    }

    public void setWaitForIndexingTimeout(Duration waitForIndexingTimeout) {
        this.waitForIndexingTimeout = waitForIndexingTimeout;
    }

    public static GetDocumentStoreOptions withTimeout(Duration duration) {
        GetDocumentStoreOptions options = new GetDocumentStoreOptions();
        options.setWaitForIndexingTimeout(duration);
        return options;
    }
}
