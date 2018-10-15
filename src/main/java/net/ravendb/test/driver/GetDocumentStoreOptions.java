package net.ravendb.test.driver;

import java.time.Duration;

@SuppressWarnings({"WeakerAccess", "unused"})
public class GetDocumentStoreOptions {
    static GetDocumentStoreOptions INSTANCE = new GetDocumentStoreOptions();

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
