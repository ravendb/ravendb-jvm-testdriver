package net.ravendb.test.driver;

import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.client.primitives.EventHandler;
import net.ravendb.client.primitives.VoidArgs;
import net.ravendb.client.serverwide.operations.GetDatabaseNamesOperation;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class DriverLifecycleTest extends RavenTestDriver {

    @Test
    public void closeClosesStoresAndDeletesTheirDatabases() {
        String database;

        try (LifecycleDriver driver = new LifecycleDriver()) {
            database = driver.openStore().getDatabase();

            assertThat(databaseNames()).contains(database);
        }

        assertThat(databaseNames()).doesNotContain(database);
    }

    @Test
    public void closingAStoreDeletesItsDatabase() {
        String database;

        try (IDocumentStore store = getDocumentStore()) {
            database = store.getDatabase();
        }

        assertThat(databaseNames()).doesNotContain(database);
    }

    @Test
    public void isDisposedReflectsClose() {
        LifecycleDriver driver = new LifecycleDriver();

        assertThat(driver.isDisposed()).isFalse();

        driver.close();

        assertThat(driver.isDisposed()).isTrue();
    }

    @Test
    public void closeIsIdempotent() {
        LifecycleDriver driver = new LifecycleDriver();
        AtomicInteger calls = new AtomicInteger();
        driver.addDriverClosedListener((sender, event) -> calls.incrementAndGet());

        driver.close();
        driver.close();

        assertThat(calls).hasValue(1);
    }

    @Test
    public void closedListenersAreNotifiedWithTheDriverAsSender() {
        LifecycleDriver driver = new LifecycleDriver();
        List<Object> senders = new ArrayList<>();
        driver.addDriverClosedListener((sender, event) -> senders.add(sender));
        driver.addDriverClosedListener((sender, event) -> senders.add(sender));

        driver.close();

        assertThat(senders).containsExactly(driver, driver);
    }

    @Test
    public void removedClosedListenerIsNotNotified() {
        LifecycleDriver driver = new LifecycleDriver();
        AtomicBoolean called = new AtomicBoolean();
        EventHandler<VoidArgs> listener = (sender, event) -> called.set(true);
        driver.addDriverClosedListener(listener);
        driver.removeDriverClosedListener(listener);

        driver.close();

        assertThat(called).isFalse();
    }

    @Test
    public void closedListenerCanUnsubscribeItselfWhileBeingNotified() {
        LifecycleDriver driver = new LifecycleDriver();
        AtomicBoolean secondCalled = new AtomicBoolean();
        driver.addDriverClosedListener(new EventHandler<VoidArgs>() {
            @Override
            public void handle(Object sender, VoidArgs event) {
                driver.removeDriverClosedListener(this);
            }
        });
        driver.addDriverClosedListener((sender, event) -> secondCalled.set(true));

        driver.close();

        assertThat(secondCalled).isTrue();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void deprecatedOnDriverClosedStillFires() {
        LifecycleDriver driver = new LifecycleDriver();
        AtomicBoolean called = new AtomicBoolean();
        driver.onDriverClosed = d -> called.set(d.isDisposed());

        driver.close();

        assertThat(called)
                .as("fires after the driver is marked disposed")
                .isTrue();
        assertThat(driver.disposed).isTrue();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void settingTheDeprecatedDisposedFieldDoesNotSkipCleanup() {
        String database;

        try (LifecycleDriver driver = new LifecycleDriver()) {
            database = driver.openStore().getDatabase();
            driver.disposed = true;
        }

        assertThat(databaseNames()).doesNotContain(database);
    }

    @Test
    public void closeClosesTheDatabaseDumpStream() {
        TrackingInputStream stream = new TrackingInputStream();
        LifecycleDriver driver = new LifecycleDriver() {
            @Override
            protected InputStream getDatabaseDumpFileStream() {
                return stream;
            }
        };

        driver.close();

        assertThat(stream.closed).isTrue();
    }

    @Test
    public void closeAggregatesEveryFailureAndStillCompletes() {
        LifecycleDriver driver = new LifecycleDriver();
        IllegalStateException first = new IllegalStateException("first");
        IllegalStateException second = new IllegalStateException("second");
        List<RuntimeException> failures = new ArrayList<>(Arrays.asList(first, second));

        for (int i = 0; i < 2; i++) {
            // registered after the driver's own listener, so the database is still deleted
            driver.openStore().addAfterCloseListener((sender, event) -> {
                throw failures.remove(0);
            });
        }

        AtomicBoolean notified = new AtomicBoolean();
        driver.addDriverClosedListener((sender, event) -> notified.set(true));

        RuntimeException thrown = catchThrowableOfType(RuntimeException.class, driver::close);

        assertThat(thrown.getCause()).isIn(first, second);
        assertThat(thrown.getSuppressed()).hasSize(1);
        assertThat(thrown).hasMessageContaining("first").hasMessageContaining("second");
        assertThat(driver.isDisposed()).isTrue();
        assertThat(notified).isTrue();
    }

    @Test
    public void setupDatabaseRunsBeforeTheStoreIsHandedOver() {
        try (LifecycleDriver driver = new LifecycleDriver()) {
            driver.seed = true;

            try (IDocumentStore store = driver.openStore();
                 IDocumentSession session = store.openSession()) {
                assertThat(session.load(BasicTest.Person.class, "people/seed"))
                        .isNotNull();
            }
        }
    }

    @Test
    public void preInitializeCanCustomizeTheStoreConventions() {
        try (LifecycleDriver driver = new LifecycleDriver()) {
            driver.maxRequestsPerSession = 7;

            try (IDocumentStore store = driver.openStore()) {
                assertThat(store.getConventions().getMaxNumberOfRequestsPerSession())
                        .isEqualTo(7);
            }
        }
    }

    private List<String> databaseNames() {
        try (IDocumentStore store = getDocumentStore("names")) {
            return Arrays.asList(store.maintenance().server().send(new GetDatabaseNamesOperation(0, Integer.MAX_VALUE)));
        }
    }

    static class LifecycleDriver extends RavenTestDriver {

        boolean seed;
        Integer maxRequestsPerSession;

        IDocumentStore openStore() {
            return getDocumentStore("lifecycle");
        }

        @Override
        protected void preInitialize(IDocumentStore documentStore) {
            if (maxRequestsPerSession != null) {
                documentStore.getConventions().setMaxNumberOfRequestsPerSession(maxRequestsPerSession);
            }
        }

        @Override
        protected void setupDatabase(IDocumentStore documentStore) {
            if (!seed) {
                return;
            }

            try (IDocumentSession session = documentStore.openSession()) {
                BasicTest.Person person = new BasicTest.Person();
                person.setName("Seed");
                session.store(person, "people/seed");
                session.saveChanges();
            }
        }
    }

    private static class TrackingInputStream extends ByteArrayInputStream {

        boolean closed;

        TrackingInputStream() {
            super(new byte[0]);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
