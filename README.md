# RavenDB Java Test Driver
You're looking at the RavenDB Test Driver for the JVM, released on Maven Central.  
It makes it easy to write tests that require a RavenDB instance, using the embedded server and a friendly API.

RavenDB is a NoSQL database that fuses extreme performance with ease-of-use, offering above the roof developer experience.  
Learn more at https://ravendb.net or visit our [GitHub repository](https://github.com/ravendb/ravendb-jvm-testdriver).


## Installation
Get the latest stable version from [Maven Central](https://central.sonatype.com/artifact/net.ravendb/ravendb-test-driver).

Maven:
```xml
<dependency>
    <groupId>net.ravendb</groupId>
    <artifactId>ravendb-test-driver</artifactId>
    <version>LATEST_VERSION</version>
    <scope>test</scope>
</dependency>
```

Gradle:
```groovy
testImplementation 'net.ravendb:ravendb-test-driver:LATEST_VERSION'
```

The embedded server requires the .NET runtime to be installed on the machine running the tests.

## Learning resources
- **Documentation** - https://docs.ravendb.net/7.2/start/test-driver?lang=java 🎒
- Java client - https://github.com/ravendb/ravendb-jvm-client 🧑‍💻
- Code Samples - https://demo.ravendb.net/ 🧑‍💻

## Community
- **RavenDB's Discord** - https://discord.gg/ravendb 🍻
- Articles & guides - https://ravendb.net/articles 📰
- YouTube - https://www.youtube.com/@ravendb_net 🍿
- Blog - https://ayende.com/blog/ 🧑‍💻


## Getting started

### Configure
```java
TestServerOptions testServerOptions = new TestServerOptions();

// Looks for the newest version on your machine including 10.0.9 and any newer patches
// but not major new releases (default is .NET version at time of server release).
testServerOptions.setFrameworkVersion("10.0.9+");

// Specifies where ravendb server binaries are located - a server zip or an extracted
// server directory (Optional, by default the server bundled in the jar is used)
testServerOptions.withExternalServer("PATH_TO_RAVENDB_SERVER");

// Specifies where ravendb data will be placed/located (Optional)
testServerOptions.setDataDirectory("PATH_TO_RAVENDB_DATADIR");

configureServer(testServerOptions);
```

`TestServerOptions` makes the server fail to start when no valid license is available. Provide one through
`testServerOptions.getLicensing().setLicense(...)` / `setLicensePath(...)`, or the `RAVEN_License` environment variable.
This also applies when `configureServer` is never called: the default server uses `TestServerOptions` too, so a
license is always required (a free developer license works).

### Write a test
```java
import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.indexes.AbstractIndexCreationTask;
import net.ravendb.client.documents.indexes.FieldIndexing;
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.test.driver.RavenTestDriver;
import net.ravendb.test.driver.TestServerOptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RavenDBTestDriver extends RavenTestDriver {

    static {
        // configureServer() must be set before calling getDocumentStore()
        // and can only be set once per test run.
        TestServerOptions testServerOptions = new TestServerOptions();
        testServerOptions.setDataDirectory("C:\\RavenDBTestDir");
        configureServer(testServerOptions);
    }

    // This allows us to modify the conventions of the store we get from 'getDocumentStore'
    @Override
    protected void preInitialize(IDocumentStore documentStore) {
        documentStore.getConventions().setMaxNumberOfRequestsPerSession(50);
    }

    @Test
    public void myFirstTest() {
        // getDocumentStore() evokes the Document Store, which establishes and manages communication
        // between your client application and a RavenDB cluster via HTTP requests.
        try (IDocumentStore store = getDocumentStore()) {
            store.executeIndex(new TestDocumentByName());

            try (IDocumentSession session = store.openSession()) {
                session.store(new TestDocument("Hello world!"));
                session.store(new TestDocument("Goodbye..."));
                session.saveChanges();
            }

            // If we want to query documents, sometimes we need to wait for the indexes to catch up
            // to prevent using stale indexes.
            waitForIndexing(store);

            // Sometimes we want to debug the test itself. This method redirects us to the studio
            // so that we can see if the code worked as expected (in this case, created two documents).
            // It only stops the test when a debugger is attached.
            waitForUserToContinueTheTest(store);

            // Queries are defined in the session scope.
            // If there is no relevant index to quickly answer the query, RavenDB creates an auto-index
            // based on the query parameters.
            // This query will use the static index defined below and filter the results by name.
            try (IDocumentSession session = store.openSession()) {
                List<TestDocument> query = session.query(TestDocument.class, TestDocumentByName.class)
                        .whereEquals("name", "hello")
                        .toList();

                assertEquals(1, query.size());
            }
        }
    }

    // AbstractIndexCreationTask allows you to create and manually define a static index.
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
```


## Contributing
- Submit an issue - https://github.com/ravendb/ravendb-jvm-testdriver/issues
- GitHub Discussions - https://github.com/ravendb/ravendb/discussions
- Contributing rules - https://github.com/ravendb/ravendb/blob/v7.2/CONTRIBUTING.md
