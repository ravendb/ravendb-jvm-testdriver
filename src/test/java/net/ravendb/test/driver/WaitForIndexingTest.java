package net.ravendb.test.driver;

import com.google.common.base.Stopwatch;
import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.indexes.IndexDefinition;
import net.ravendb.client.documents.indexes.IndexState;
import net.ravendb.client.documents.operations.GetStatisticsOperation;
import net.ravendb.client.documents.operations.IndexInformation;
import net.ravendb.client.documents.operations.indexes.DisableIndexOperation;
import net.ravendb.client.documents.operations.indexes.PutIndexesOperation;
import net.ravendb.client.documents.operations.indexes.StopIndexingOperation;
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.client.exceptions.TimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

@SuppressWarnings("deprecation")
public class WaitForIndexingTest extends RavenTestDriver {

    private static final String BY_NAME = "People/ByName";
    private static final String BY_NAME_MAP = "from p in docs.People select new { p.name }";
    private static final String STALE_BY_NAME_MESSAGE = "The indexes stayed stale for more than 00:00:01."
            + "Indexing errors:\r\nIndex '" + BY_NAME + "' (0 errors):\r\n";
    private static final String FAULTY = "People/Faulty";
    private static final String FAULTY_MAP = "from p in docs.People select new { Number = 1 / (p.name.Length - p.name.Length) }";

    @Test
    public void returnsOnceIndexesAreNotStale() {
        try (IDocumentStore store = getDocumentStore()) {
            putIndex(store, BY_NAME, BY_NAME_MAP);
            storePeople(store, 100);

            waitForIndexing(store);

            assertThat(store.maintenance().send(new GetStatisticsOperation()).getStaleIndexes())
                    .isEmpty();
        }
    }

    @Test
    public void disabledIndexesAreIgnored() {
        try (IDocumentStore store = getDocumentStore()) {
            putIndex(store, BY_NAME, BY_NAME_MAP);
            store.maintenance().send(new DisableIndexOperation(BY_NAME));
            storePeople(store, 10);

            assertThatCode(() -> waitForIndexing(store, null, Duration.ofSeconds(10)))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    public void erroredButNotStaleIndexCountsAsDone() throws Exception {
        // mirrors C#: only disabled indexes are filtered out, and an errored index stops being stale
        try (IDocumentStore store = getDocumentStore()) {
            putIndex(store, FAULTY, FAULTY_MAP);
            storePeople(store, 1);
            awaitIndexState(store, FAULTY, IndexState.ERROR);

            assertThatCode(() -> waitForIndexing(store, null, Duration.ofSeconds(10)))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    public void erroredIndexFailsFastWithTheIndexingErrors() throws Exception {
        try (IDocumentStore store = getDocumentStore()) {
            putIndex(store, FAULTY, FAULTY_MAP);
            storePeople(store, 1);
            awaitIndexState(store, FAULTY, IndexState.ERROR);

            // a second index that can never catch up keeps the database stale
            store.maintenance().send(new StopIndexingOperation());
            putIndex(store, BY_NAME, BY_NAME_MAP);

            Stopwatch sw = Stopwatch.createStarted();
            Throwable thrown = catchThrowable(() -> waitForIndexing(store, null, Duration.ofSeconds(30)));

            assertThat(sw.elapsed(TimeUnit.SECONDS))
                    .as("an errored index must end the wait without running out the timeout")
                    .isLessThan(10);
            assertThat(thrown)
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageStartingWith("The indexes stayed stale for more than 00:00:30.Indexing errors:\r\n")
                    .hasMessageContaining("Index '" + FAULTY + "' (1 errors):\r\n- ");
        }
    }

    @Test
    public void staleIndexTimesOutListingEveryIndexEvenWithoutErrors() {
        // mirrors C#: GetIndexErrorsOperation returns an entry per index, so the header is always there
        try (IDocumentStore store = getDocumentStore()) {
            store.maintenance().send(new StopIndexingOperation());
            putIndex(store, BY_NAME, BY_NAME_MAP);
            storePeople(store, 10);

            assertThat(catchThrowable(() -> waitForIndexing(store, null, Duration.ofSeconds(1))))
                    .isInstanceOf(TimeoutException.class)
                    .hasMessage(STALE_BY_NAME_MESSAGE);
        }
    }

    @Test
    public void getDocumentStoreWaitsForIndexesCreatedInSetupDatabase() {
        try (IndexedDriver driver = new IndexedDriver()) {
            try (IDocumentStore store = driver.openStore(GetDocumentStoreOptions.withTimeout(Duration.ofSeconds(30)))) {
                IndexInformation[] indexes = store.maintenance().send(new GetStatisticsOperation()).getIndexes();

                assertThat(indexes).hasSize(1);
                assertThat(indexes[0].isStale()).isFalse();
            }
        }
    }

    @Test
    public void getDocumentStoreFailsWhenIndexesFromSetupDatabaseStayStale() {
        try (IndexedDriver driver = new IndexedDriver()) {
            driver.stopIndexing = true;

            assertThat(catchThrowable(() -> driver.openStore(GetDocumentStoreOptions.withTimeout(Duration.ofSeconds(1)))))
                    .isInstanceOf(TimeoutException.class)
                    .hasMessage(STALE_BY_NAME_MESSAGE);
        }
    }

    private static void awaitIndexState(IDocumentStore store, String name, IndexState state) throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        while (sw.elapsed(TimeUnit.SECONDS) < 30) {
            for (IndexInformation index : store.maintenance().send(new GetStatisticsOperation()).getIndexes()) {
                if (name.equals(index.getName()) && state.equals(index.getState())) {
                    return;
                }
            }
            Thread.sleep(100);
        }

        throw new AssertionError("Index '" + name + "' did not reach state " + state);
    }

    static void putIndex(IDocumentStore store, String name, String map) {
        IndexDefinition definition = new IndexDefinition();
        definition.setName(name);
        definition.setMaps(Collections.singleton(map));

        store.maintenance().send(new PutIndexesOperation(definition));
    }

    static void storePeople(IDocumentStore store, int count) {
        try (IDocumentSession session = store.openSession()) {
            for (int i = 1; i <= count; i++) {
                BasicTest.Person person = new BasicTest.Person();
                person.setName("Person " + i);
                session.store(person, "people/" + i);
            }
            session.saveChanges();
        }
    }

    private static class IndexedDriver extends RavenTestDriver {

        boolean stopIndexing;

        IDocumentStore openStore(GetDocumentStoreOptions options) {
            return getDocumentStore(options, "indexed");
        }

        @Override
        protected void setupDatabase(IDocumentStore documentStore) {
            if (stopIndexing) {
                documentStore.maintenance().send(new StopIndexingOperation());
            }
            putIndex(documentStore, BY_NAME, BY_NAME_MAP);
            storePeople(documentStore, 50);
        }
    }
}
