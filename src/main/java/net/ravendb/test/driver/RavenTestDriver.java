package net.ravendb.test.driver;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import net.ravendb.client.Constants;
import net.ravendb.client.documents.DocumentStore;
import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.Lazy;
import net.ravendb.client.documents.indexes.IndexErrors;
import net.ravendb.client.documents.indexes.IndexState;
import net.ravendb.client.documents.operations.DatabaseStatistics;
import net.ravendb.client.documents.operations.GetStatisticsOperation;
import net.ravendb.client.documents.operations.IndexInformation;
import net.ravendb.client.documents.operations.MaintenanceOperationExecutor;
import net.ravendb.client.documents.operations.indexes.GetIndexErrorsOperation;
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.client.documents.smuggler.DatabaseSmugglerImportOptions;
import net.ravendb.client.exceptions.RavenException;
import net.ravendb.client.exceptions.TimeoutException;
import net.ravendb.client.exceptions.cluster.NoLeaderException;
import net.ravendb.client.exceptions.database.DatabaseDoesNotExistException;
import net.ravendb.client.http.RequestExecutor;
import net.ravendb.client.primitives.CleanCloseable;
import net.ravendb.client.serverwide.DatabaseRecord;
import net.ravendb.client.serverwide.operations.CreateDatabaseOperation;
import net.ravendb.client.serverwide.operations.DeleteDatabasesOperation;
import net.ravendb.client.util.UrlUtils;
import net.ravendb.embedded.CommandLineArgumentEscaper;
import net.ravendb.embedded.EmbeddedServer;
import net.ravendb.embedded.ServerOptions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.routing.DefaultProxyRoutePlanner;
import org.apache.hc.core5.http.HttpHost;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings({"WeakerAccess", "unused", "Convert2MethodRef"})
public class RavenTestDriver implements CleanCloseable {
    private static final EmbeddedServer TEST_SERVER = new EmbeddedServer();

    private static final Lazy<IDocumentStore> TEST_SERVER_STORE = new Lazy<>(() -> runServer());

    private final ConcurrentMap<DocumentStore, Boolean> _documentStores = new ConcurrentHashMap<>();

    private static AtomicInteger _index = new AtomicInteger(0);
    private static ServerOptions _globalServerOptions;

    private static File _emptySettingsFile;

    private static File getEmptySettingsFile() throws IOException {
        if (_emptySettingsFile == null) {
            _emptySettingsFile = File.createTempFile("settings-", ".json");
            FileUtils.write(_emptySettingsFile, "{}", Charset.defaultCharset());
            _emptySettingsFile.deleteOnExit();
        }
        return _emptySettingsFile;
    }

    protected String getDatabaseDumpFilePath() {
        return null;
    }

    protected InputStream getDatabaseDumpFileStream() {
        return null;
    }

    protected boolean disposed;

    private static Supplier<ServerOptions> serverOptions = () -> defaultServerOptions();

    public static void configureServer(ServerOptions options) {
        if (TEST_SERVER_STORE.isValueCreated()) {
            throw new IllegalStateException("Cannot configure server after it was started. " +
                    "Please call 'configureServer' method before any 'getDocumentStore' is called.");
        }

        _globalServerOptions = options;
    }

    protected IDocumentStore getDocumentStore() {
        return getDocumentStore(null, null);
    }

    protected IDocumentStore getDocumentStore(GetDocumentStoreOptions options) {
        return getDocumentStore(options, null);
    }

    protected IDocumentStore getDocumentStore(String database) {
        return getDocumentStore(null, database);
    }

    protected IDocumentStore getDocumentStore(GetDocumentStoreOptions options, String database) {
        database = ObjectUtils.firstNonNull(database, "test");
        options = ObjectUtils.firstNonNull(options, GetDocumentStoreOptions.INSTANCE);
        String name = database + "_" + _index.incrementAndGet();
        IDocumentStore documentStore = TEST_SERVER_STORE.getValue();

        CreateDatabaseOperation createDatabaseOperation = new CreateDatabaseOperation(new DatabaseRecord(name));
        documentStore.maintenance().server().send(createDatabaseOperation);

        DocumentStore store = new DocumentStore(documentStore.getUrls(), name);

        preInitialize(store);
        store.initialize();

        store.addAfterCloseListener((sender, event) -> {
            Boolean value = _documentStores.remove(store);

            if (!value) {
                return;
            }

            try {
                store.maintenance().server().send(new DeleteDatabasesOperation(store.getDatabase(), true));
            } catch (DatabaseDoesNotExistException | NoLeaderException e) {
                // ignore
            }
        });

        try {
            importDatabase(store, name);
        } catch (IOException e) {
            throw new RavenException("Unable to import database: " + e.getMessage(), e);
        }

        setupDatabase(store);

        if (options.getWaitForIndexingTimeout() != null) {
            waitForIndexing(store, name, options.getWaitForIndexingTimeout());
        }

        _documentStores.put(store, true);

        return store;
    }

    protected void preInitialize(IDocumentStore documentStore) {
        // empty by design
    }

    protected void setupDatabase(IDocumentStore documentStore) {
        // empty by design
    }

    protected Consumer<RavenTestDriver> onDriverClosed = (driver) -> {};


    public static void waitForIndexing(IDocumentStore store) {
        waitForIndexing(store, null, null);
    }

    public static void waitForIndexing(IDocumentStore store, String database) {
        waitForIndexing(store, database, null);
    }

    public static void waitForIndexing(IDocumentStore store, String database, Duration timeout) {
        MaintenanceOperationExecutor admin = store.maintenance().forDatabase(database);

        if (timeout == null) {
            timeout = Duration.ofMinutes(1);
        }

        Stopwatch sp = Stopwatch.createStarted();

        while (sp.elapsed(TimeUnit.MILLISECONDS) < timeout.toMillis()) {
            DatabaseStatistics databaseStatistics = admin.send(new GetStatisticsOperation());

            List<IndexInformation> indexes = Arrays.stream(databaseStatistics.getIndexes())
                    .filter(x -> !IndexState.DISABLED.equals(x.getState()))
                    .collect(Collectors.toList());

            if (indexes.stream().allMatch(x -> !x.isStale() &&
                    !x.getName().startsWith(Constants.Documents.Indexing.SIDE_BY_SIDE_INDEX_NAME_PREFIX))) {
                return;
            }

            if (Arrays.stream(databaseStatistics.getIndexes()).anyMatch(x -> IndexState.ERROR.equals(x.getState()))) {
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        IndexErrors[] errors = admin.send(new GetIndexErrorsOperation());
        String allIndexErrorsText = "";
        Function<IndexErrors, String> formatIndexErrors = indexErrors -> {
            String errorsListText = Arrays.stream(indexErrors.getErrors()).map(x -> "-" + x).collect(Collectors.joining(System.lineSeparator()));
            return "Index " + indexErrors.getName() + " (" + indexErrors.getErrors().length + " errors): "+ System.lineSeparator() + errorsListText;
        };
        if (errors != null && errors.length > 0) {
            allIndexErrorsText = Arrays.stream(errors).map(formatIndexErrors).collect(Collectors.joining(System.lineSeparator()));
        }

        throw new TimeoutException("The indexes stayed stale for more than " + timeout + "." + allIndexErrorsText);
    }

    protected void waitForUserToContinueTheTest(IDocumentStore store) {
        String databaseNameEncoded = UrlUtils.escapeDataString(store.getDatabase());
        String documentsPage = store.getUrls()[0] + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

        openBrowser(documentsPage);

        do {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }

            try (IDocumentSession session = store.openSession()) {
                if (session.load(ObjectNode.class, "Debug/Done") != null) {
                    break;
                }
            }

        } while (true);
    }

    protected void openBrowser(String url) {
        System.out.println(url);

        if (Desktop.isDesktopSupported()) {
            Desktop desktop = Desktop.getDesktop();
            try {
                desktop.browse(new URI(url));
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else {
            Runtime runtime = Runtime.getRuntime();
            try {
                runtime.exec("xdg-open " + url);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {
        if (disposed) {
            return;
        }

        ArrayList<Exception> exceptions = new ArrayList<>();

        for (DocumentStore documentStore : _documentStores.keySet()) {
            try {
                documentStore.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        disposed = true;

        if (onDriverClosed != null) {
            onDriverClosed.accept(this);
        }

        if (exceptions.size() > 0) {
            throw new RuntimeException(exceptions.stream()
                    .map(x -> x.toString()).collect(Collectors.joining(", ")));
        }
    }

    private static void cleanupTempDirs(File... dirs) {
        try {
            // try up to 30 times in 200 ms intervals
            for (int i = 0; i < 30; i++) {
                boolean anyFailure = false;
                for (File dir : dirs) {
                    if (dir.exists()) {
                        if (!FileUtils.deleteQuietly(dir)) {
                            anyFailure = true;
                        }
                    }
                }

                if (!anyFailure) {
                    return;
                }

                Thread.sleep(200);
            }
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static ServerOptions defaultServerOptions() {
        ServerOptions options = new ServerOptions();

        File serverDir = Files.createTempDir();
        serverDir.deleteOnExit();

        File dataDir = Files.createTempDir();
        dataDir.deleteOnExit();

        File logsDir = Files.createTempDir();
        logsDir.deleteOnExit();

        options.setDataDirectory(dataDir.getAbsolutePath());
        options.setTargetServerLocation(serverDir.getAbsolutePath());
        options.setLogsPath(logsDir.getAbsolutePath());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupTempDirs(serverDir, dataDir, logsDir);
        }));

        return options;
    }

    private void importDatabase(DocumentStore docStore, String database) throws IOException {
        DatabaseSmugglerImportOptions options = new DatabaseSmugglerImportOptions();
        if (getDatabaseDumpFilePath() != null) {
            docStore.smuggler().forDatabase(database)
                    .importAsync(options, getDatabaseDumpFilePath());
        } else if (getDatabaseDumpFileStream() != null) {
            docStore.smuggler().forDatabase(database)
                    .importAsync(options, getDatabaseDumpFileStream());
        }
    }

    private static IDocumentStore runServer() {
        try {
            ServerOptions options = ObjectUtils.firstNonNull(_globalServerOptions, serverOptions.get());

            List<String> commandLineArgs = options.getCommandLineArgs();

            commandLineArgs.add(0,"-c");
            commandLineArgs.add(1, CommandLineArgumentEscaper.escapeSingleArg(getEmptySettingsFile().getAbsolutePath()));
            commandLineArgs.add("--RunInMemory=true");

            TEST_SERVER.startServer(options);

            String url = TEST_SERVER.getServerUri();

            DocumentStore store = new DocumentStore(url, null);

            store.initialize();

            return store;
        } catch (IOException e) {
            throw new RavenException("Unable to start server: " + e.getMessage(), e);
        }
    }

    public void withFiddler() {
        RequestExecutor.configureHttpClient = builder -> {
            HttpHost proxy = new HttpHost("http", "127.0.0.1", 8888);
            builder.setRoutePlanner(new DefaultProxyRoutePlanner(proxy));
        };
    }
}
