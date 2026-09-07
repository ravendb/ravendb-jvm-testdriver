package net.ravendb.test.driver;

import net.ravendb.client.documents.BulkInsertOperation;
import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.operations.GetStatisticsOperation;
import net.ravendb.client.documents.operations.Operation;
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.client.documents.smuggler.DatabaseSmugglerExportOptions;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the getDatabaseDumpFilePath() path end to end: a dump is produced from one test
 * database and must be fully visible in the store handed back for another one.
 * <p>
 * Note this does not reproduce the un-awaited import race by itself - against a local
 * in-memory server the import finishes while importAsync is still uploading, which is why
 * the defect stayed unnoticed. It does guard the path against breaking, and fails if
 * waitForCompletion() ever starts throwing or hanging.
 */
public class DatabaseDumpImportTest extends RavenTestDriver {

    private static final int NUMBER_OF_DOCUMENTS = 5000;

    @Test
    public void dumpIsFullyImportedBeforeTheTestBodyRuns() throws Exception {
        File dumpFile = createDump();

        try (DumpDriver driver = new DumpDriver(dumpFile.getAbsolutePath())) {
            try (IDocumentStore store = driver.openStore()) {
                assertThat(store.maintenance().send(new GetStatisticsOperation()).getCountOfDocuments())
                        .isGreaterThanOrEqualTo(NUMBER_OF_DOCUMENTS);

                try (IDocumentSession session = store.openSession()) {
                    // a document from the tail of the dump
                    BasicTest.Person person = session.load(BasicTest.Person.class,
                            "people/" + NUMBER_OF_DOCUMENTS);

                    assertThat(person)
                            .as("the whole dump must be imported before the store is handed over")
                            .isNotNull();
                    assertThat(person.getName())
                            .isEqualTo("Person " + NUMBER_OF_DOCUMENTS);
                }
            }
        }
    }

    private File createDump() throws Exception {
        File dumpFile = File.createTempFile("test-driver-dump-", ".ravendbdump");
        dumpFile.deleteOnExit();

        try (IDocumentStore store = getDocumentStore("dumpSource")) {
            try (BulkInsertOperation bulkInsert = store.bulkInsert()) {
                for (int i = 1; i <= NUMBER_OF_DOCUMENTS; i++) {
                    BasicTest.Person person = new BasicTest.Person();
                    person.setName("Person " + i);
                    bulkInsert.store(person, "people/" + i);
                }
            }

            Operation operation = store.smuggler()
                    .forDatabase(store.getDatabase())
                    .exportAsync(new DatabaseSmugglerExportOptions(), dumpFile.getAbsolutePath());
            operation.waitForCompletion();
        }

        return dumpFile;
    }

    private static class DumpDriver extends RavenTestDriver {

        private final String dumpFilePath;

        DumpDriver(String dumpFilePath) {
            this.dumpFilePath = dumpFilePath;
        }

        IDocumentStore openStore() {
            return getDocumentStore("dumpTarget");
        }

        @Override
        protected String getDatabaseDumpFilePath() {
            return dumpFilePath;
        }
    }
}
