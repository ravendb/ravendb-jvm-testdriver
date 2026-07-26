package net.ravendb.test.driver;

import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.serverwide.DatabaseRecord;
import net.ravendb.client.serverwide.operations.GetDatabaseRecordOperation;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class DriverBehaviorTest extends RavenTestDriver {

    @Test
    public void databaseIsNamedAfterTheCallingMethod() {
        try (IDocumentStore store = getDocumentStore()) {
            assertThat(store.getDatabase())
                    .startsWith("databaseIsNamedAfterTheCallingMethod_");
        }
    }

    @Test
    public void explicitDatabaseNameWins() {
        try (IDocumentStore store = getDocumentStore("explicit")) {
            assertThat(store.getDatabase())
                    .startsWith("explicit_");
        }
    }

    @Test
    public void closingAStoreTwiceDoesNotThrow() {
        IDocumentStore store = getDocumentStore();

        store.close();

        // the JVM client fires afterClose on every call, so the driver's listener runs again
        // with the store already gone from its map
        assertThatCode(store::close)
                .doesNotThrowAnyException();
    }

    @Test
    public void waitForUserToContinueTheTestReturnsWithoutADebugger() throws Exception {
        assumeFalse(isDebugAgentPresent(), "the JVM was started with a debug agent");

        try (IDocumentStore store = getDocumentStore()) {
            AtomicBoolean returned = new AtomicBoolean();

            // run it on another thread so a regression fails the test instead of hanging the build
            Thread thread = new Thread(() -> {
                waitForUserToContinueTheTest(store);
                returned.set(true);
            });
            thread.setDaemon(true);
            thread.start();
            thread.join(30_000);

            assertThat(returned.get())
                    .as("waitForUserToContinueTheTest must be a no-op when no debugger is attached")
                    .isTrue();
        }
    }

    @Test
    public void preConfigureDatabaseCanCustomizeTheDatabaseRecord() {
        try (SettingsDriver driver = new SettingsDriver()) {
            try (IDocumentStore store = driver.openStore()) {
                DatabaseRecord record = store.maintenance().server()
                        .send(new GetDatabaseRecordOperation(store.getDatabase()));

                assertThat(record.getSettings())
                        .containsEntry("Databases.QueryTimeoutInSec", "123");
            }
        }
    }

    private static boolean isDebugAgentPresent() {
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (argument.startsWith("-agentlib:jdwp") || argument.startsWith("-Xrunjdwp")) {
                return true;
            }
        }

        return false;
    }

    private static class SettingsDriver extends RavenTestDriver {

        IDocumentStore openStore() {
            return getDocumentStore("preConfigured");
        }

        @Override
        protected void preConfigureDatabase(DatabaseRecord databaseRecord) {
            databaseRecord.getSettings().put("Databases.QueryTimeoutInSec", "123");
        }
    }
}
