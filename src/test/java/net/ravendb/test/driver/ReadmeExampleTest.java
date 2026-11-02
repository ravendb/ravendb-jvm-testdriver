package net.ravendb.test.driver;

import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.indexes.AbstractIndexCreationTask;
import net.ravendb.client.documents.indexes.FieldIndexing;
import net.ravendb.client.documents.session.IDocumentSession;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The README's "Write a test" example, kept compiling and passing. Its static configureServer block is
 * left out: the server is shared by every test in this JVM and can be configured only once.
 */
@SuppressWarnings("deprecation")
public class ReadmeExampleTest extends RavenTestDriver {

    @Override
    protected void preInitialize(IDocumentStore documentStore) {
        documentStore.getConventions().setMaxNumberOfRequestsPerSession(50);
    }

    @Test
    public void myFirstTest() {
        try (IDocumentStore store = getDocumentStore()) {
            store.executeIndex(new TestDocumentByName());

            try (IDocumentSession session = store.openSession()) {
                session.store(new TestDocument("Hello world!"));
                session.store(new TestDocument("Goodbye..."));
                session.saveChanges();
            }

            waitForIndexing(store);

            waitForUserToContinueTheTest(store);

            try (IDocumentSession session = store.openSession()) {
                List<TestDocument> query = session.query(TestDocument.class, TestDocumentByName.class)
                        .whereEquals("name", "hello")
                        .toList();

                assertEquals(1, query.size());
            }
        }
    }

    public static class TestDocumentByName extends AbstractIndexCreationTask {
        public TestDocumentByName() {
            map = "from doc in docs.TestDocuments select new { doc.name }";
            index("name", FieldIndexing.SEARCH);
        }
    }

    public static class TestDocument {
        private String name;

        public TestDocument() {
        }

        public TestDocument(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
