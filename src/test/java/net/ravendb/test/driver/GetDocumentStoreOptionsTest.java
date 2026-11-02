package net.ravendb.test.driver;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class GetDocumentStoreOptionsTest {

    @Test
    public void defaultIsSharedAndDoesNotWaitForIndexing() {
        assertThat(GetDocumentStoreOptions.getDefault())
                .isSameAs(GetDocumentStoreOptions.getDefault());
        assertThat(GetDocumentStoreOptions.getDefault().getWaitForIndexingTimeout())
                .isNull();
    }

    @Test
    public void withTimeoutCreatesANewInstance() {
        GetDocumentStoreOptions options = GetDocumentStoreOptions.withTimeout(Duration.ofSeconds(5));

        assertThat(options).isNotSameAs(GetDocumentStoreOptions.getDefault());
        assertThat(options.getWaitForIndexingTimeout()).isEqualTo(Duration.ofSeconds(5));
    }
}
