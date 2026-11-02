package net.ravendb.test.driver;

import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import net.ravendb.client.Constants;
import net.ravendb.client.documents.DocumentStore;
import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.indexes.IndexErrors;
import net.ravendb.client.documents.indexes.IndexState;
import net.ravendb.client.documents.operations.DatabaseStatistics;
import net.ravendb.client.documents.operations.GetStatisticsOperation;
import net.ravendb.client.documents.operations.IndexInformation;
import net.ravendb.client.documents.operations.MaintenanceOperationExecutor;
import net.ravendb.client.documents.operations.Operation;
import net.ravendb.client.documents.operations.indexes.GetIndexErrorsOperation;
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.client.documents.smuggler.DatabaseSmugglerImportOptions;
import net.ravendb.client.exceptions.RavenException;
import net.ravendb.client.exceptions.TimeoutException;
import net.ravendb.client.exceptions.cluster.NoLeaderException;
import net.ravendb.client.exceptions.database.DatabaseDoesNotExistException;
import net.ravendb.client.http.RequestExecutor;
import net.ravendb.client.primitives.CleanCloseable;
import net.ravendb.client.primitives.EventArgs;
import net.ravendb.client.primitives.EventHandler;
import net.ravendb.client.primitives.EventHelper;
import net.ravendb.client.primitives.VoidArgs;
import net.ravendb.client.serverwide.DatabaseRecord;
import net.ravendb.client.serverwide.operations.CreateDatabaseOperation;
import net.ravendb.client.serverwide.operations.DeleteDatabasesOperation;
import net.ravendb.client.util.UrlUtils;
import net.ravendb.embedded.CommandLineArgumentEscaper;
import net.ravendb.embedded.EmbeddedServer;
import net.ravendb.embedded.ServerOptions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hc.client5.http.impl.routing.DefaultProxyRoutePlanner;
import org.apache.hc.core5.http.HttpHost;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings({"WeakerAccess", "unused", "Convert2MethodRef"})
public class RavenTestDriver implements CleanCloseable {
    private static final EmbeddedServer TEST_SERVER = new EmbeddedServer();

    private static final FailureCachingLazy<IDocumentStore> TEST_SERVER_STORE = new FailureCachingLazy<>(() -> runServer());

    private final ConcurrentMap<DocumentStore, Boolean> _documentStores = new ConcurrentHashMap<>();

    private static AtomicInteger _index = new AtomicInteger(0);
    private static ServerOptions _globalServerOptions;

    private EmbeddedServer _scopedServer;
    private ServerOptions _scopedServerOptions;
    private final FailureCachingLazy<IDocumentStore> _scopedDocumentStore = new FailureCachingLazy<>(() -> createScopedDocumentStore());

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

    private boolean _disposed;

    /**
     * @deprecated use {@link #isDisposed()}. Kept in sync for subclasses that read it, but setting it no
     * longer has any effect - like C#'s {@code IsDisposed}, the disposed state can only be changed by
     * {@link #close()}.
     */
    @Deprecated
    protected boolean disposed;

    protected boolean isDisposed() {
        return _disposed;
    }

    private static Supplier<TestServerOptions> serverOptions = () -> defaultServerOptions();

    public static void configureServer(TestServerOptions options) {
        configureServerInternal(options);
    }

    /**
     * @param options options for the global server
     * @deprecated use {@link #configureServer(TestServerOptions)}. Plain {@link ServerOptions} do not enforce a
     * valid license, so a test server configured this way silently starts unlicensed. This overload will be
     * removed in the next major version, mirroring the C# test driver.
     */
    @Deprecated
    public static void configureServer(ServerOptions options) {
        configureServerInternal(options);
    }

    private static void configureServerInternal(ServerOptions options) {
        if (TEST_SERVER_STORE.isValueCreated()) {
            throw new IllegalStateException("Cannot configure server after it was started. " +
                    "Please call 'configureServer' method before any 'getDocumentStore' is called.");
        }

        _globalServerOptions = options;
    }

    /**
     * Runs this driver instance against its own embedded server instead of the JVM-wide one, so a
     * test class can use its own server configuration without affecting every other test.
     * <p>
     * Package-private on purpose: it mirrors an {@code internal} C# member, and the repository's
     * own tests live in this package.
     *
     * @param options options for the scoped server
     * @return handle that shuts the scoped server down and returns this driver to the global one
     */
    CleanCloseable configureScopedServer(TestServerOptions options) {
        return configureScopedServer(options, null);
    }

    /**
     * @param options options for the scoped server
     * @param server  server to use, or null to create one
     * @return handle that shuts the scoped server down and returns this driver to the global one
     */
    CleanCloseable configureScopedServer(TestServerOptions options, EmbeddedServer server) {
        if (_scopedServer != null) {
            throw new IllegalStateException("Local embedded server is already set.");
        }

        if (_scopedDocumentStore.isValueCreated()) {
            throw new IllegalStateException("Cannot set embedded server after document store has been initialized.");
        }

        _scopedServer = server != null ? server : new EmbeddedServer();
        _scopedServerOptions = options;

        return () -> {
            try {
                if (_scopedServer != null) {
                    _scopedServer.close();
                }
            } catch (Exception e) {
                // swallow exceptions
            } finally {
                _scopedServer = null;
                _scopedServerOptions = null;
            }
        };
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
        if (database == null) {
            database = getCallerMemberName();
        }
        options = ObjectUtils.firstNonNull(options, GetDocumentStoreOptions.getDefault());
        String name = database + "_" + _index.incrementAndGet();
        IDocumentStore documentStore = _scopedServer != null
                ? _scopedDocumentStore.getValue()
                : TEST_SERVER_STORE.getValue();

        DatabaseRecord databaseRecord = new DatabaseRecord(name);

        preConfigureDatabase(databaseRecord);

        CreateDatabaseOperation createDatabaseOperation = new CreateDatabaseOperation(databaseRecord);
        documentStore.maintenance().server().send(createDatabaseOperation);

        DocumentStore store = new DocumentStore(documentStore.getUrls(), name);

        preInitialize(store);
        store.initialize();

        store.addAfterCloseListener((sender, event) -> {
            Boolean value = _documentStores.remove(store);

            if (!Boolean.TRUE.equals(value)) {
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
            waitForIndexingInternal(store, name, options.getWaitForIndexingTimeout());
        }

        _documentStores.put(store, true);

        return store;
    }

    /**
     * Names the test database after the method that asked for the document store, the way
     * [CallerMemberName] does on the C# side. Java has no such compiler support, so the
     * immediate caller is taken from the stack: the first frame that no longer belongs to
     * this class.
     */
    private String getCallerMemberName() {
        String driverClassName = RavenTestDriver.class.getName();
        boolean insideDriver = false;

        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (driverClassName.equals(element.getClassName())) {
                insideDriver = true;
                continue;
            }

            if (!insideDriver) {
                continue;
            }

            String methodName = element.getMethodName();
            if ("<init>".equals(methodName)) {
                return toDatabaseName(getTypeName() + "_ctor");
            }
            if ("<clinit>".equals(methodName)) {
                return toDatabaseName(getTypeName() + "_cctor");
            }

            return toDatabaseName(methodName);
        }

        return "test";
    }

    private String getTypeName() {
        String typeName = getClass().getSimpleName();
        return typeName.isEmpty() ? getClass().getName() : typeName;
    }

    /**
     * Synthetic member names (lambdas, anonymous classes) contain characters a database name
     * cannot hold, so anything outside the allowed set is replaced.
     */
    private static String toDatabaseName(String memberName) {
        return memberName.replaceAll("[^A-Za-z0-9_\\-.]", "_");
    }

    protected void preInitialize(IDocumentStore documentStore) {
        // empty by design
    }

    protected void preConfigureDatabase(DatabaseRecord databaseRecord) {
        // empty by design
    }

    protected void setupDatabase(IDocumentStore documentStore) {
        // empty by design
    }

    /**
     * @deprecated use {@link #addDriverClosedListener(EventHandler)}, which mirrors the C# DriverDisposed
     * event and supports any number of listeners. This field will be removed in the next major version.
     */
    @Deprecated
    protected Consumer<RavenTestDriver> onDriverClosed = (driver) -> {};

    private final List<EventHandler<VoidArgs>> onDriverClosedListeners = new ArrayList<>();

    /**
     * Registers a listener notified, with this driver as the sender, once the driver is closed -
     * the equivalent of the C# DriverDisposed event.
     * @param listener listener to add
     */
    protected void addDriverClosedListener(EventHandler<VoidArgs> listener) {
        onDriverClosedListeners.add(listener);
    }

    /**
     * @param listener listener to remove
     */
    protected void removeDriverClosedListener(EventHandler<VoidArgs> listener) {
        onDriverClosedListeners.remove(listener);
    }

    // TODO next major version: change the waitForIndexing overloads from 'public static' to 'protected'
    //  instance methods (mirroring C#'s 'protected void WaitForIndexing') and drop @Deprecated.
    //  No replacement is offered meanwhile: Java can't declare an instance method next to a static
    //  one with the same signature.

    /**
     * Waits until all non-disabled indexes of the store's database are up to date.
     *
     * @param store store to wait on
     * @deprecated in the next major version this becomes a {@code protected} instance method, mirroring the
     * C# test driver. Calls made from a {@link RavenTestDriver} subclass ({@code waitForIndexing(store)})
     * will keep compiling unchanged. Calls through the class from elsewhere
     * ({@code RavenTestDriver.waitForIndexing(store)}) will not - move them into a {@link RavenTestDriver}
     * subclass.
     */
    @Deprecated
    public static void waitForIndexing(IDocumentStore store) {
        waitForIndexingInternal(store, null, null);
    }

    /**
     * Waits until all non-disabled indexes of the given database are up to date.
     *
     * @param store    store to wait on
     * @param database database to wait on, or null for the store's database
     * @deprecated becomes a {@code protected} instance method in the next major version, see
     * {@link #waitForIndexing(IDocumentStore)}.
     */
    @Deprecated
    public static void waitForIndexing(IDocumentStore store, String database) {
        waitForIndexingInternal(store, database, null);
    }

    /**
     * Waits until all non-disabled indexes of the given database are up to date.
     *
     * @param store    store to wait on
     * @param database database to wait on, or null for the store's database
     * @param timeout  how long to wait, or null for one minute
     * @deprecated becomes a {@code protected} instance method in the next major version, see
     * {@link #waitForIndexing(IDocumentStore)}.
     */
    @Deprecated
    public static void waitForIndexing(IDocumentStore store, String database, Duration timeout) {
        waitForIndexingInternal(store, database, timeout);
    }

    private static void waitForIndexingInternal(IDocumentStore store, String database, Duration timeout) {
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
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        IndexErrors[] errors = admin.send(new GetIndexErrorsOperation());

        String allIndexErrorsText = "";
        if (errors != null && errors.length > 0) {
            String allIndexErrorsListText = Arrays.stream(errors)
                    .map(RavenTestDriver::formatIndexErrors)
                    .collect(Collectors.joining("\r\n"));
            allIndexErrorsText = "Indexing errors:\r\n" + allIndexErrorsListText;
        }

        throw new TimeoutException("The indexes stayed stale for more than " + timeout + "." + allIndexErrorsText);
    }

    private static String formatIndexErrors(IndexErrors indexErrors) {
        String errorsListText = Arrays.stream(indexErrors.getErrors())
                .map(x -> "- " + x)
                .collect(Collectors.joining("\r\n"));

        return "Index '" + indexErrors.getName() + "' (" + indexErrors.getErrors().length + " errors):\r\n" + errorsListText;
    }

    protected void waitForUserToContinueTheTest(IDocumentStore store) {
        if (!isDebuggerAttached()) {
            return;
        }

        String databaseNameEncoded = UrlUtils.escapeDataString(store.getDatabase());
        String documentsPage = store.getUrls()[0] + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

        openBrowser(documentsPage);

        do {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            try (IDocumentSession session = store.openSession()) {
                if (session.advanced().exists("Debug/Done")) {
                    session.delete("Debug/Done");
                    session.saveChanges();
                    break;
                }
            }

        } while (true);
    }

    private static boolean isDebuggerAttached() {
        try {
            return isDebuggerAttached(ManagementFactory.getRuntimeMXBean().getInputArguments());
        } catch (Exception e) {
            // the management interface is not always available, assume no debugger
            return false;
        }
    }

    /**
     * A loaded jdwp agent alone does not mean a debugger is attached: {@code server=y,suspend=n} (the
     * usual "remote debugging enabled" setup of CI agents and containers) only listens, and the JVM runs
     * whether anyone connects or not. Every other mode implies a debugger was attached at startup:
     * <ul>
     *     <li>{@code server=n} (the default, used by IntelliJ, Eclipse and VS Code) - the JVM connects
     *     to the debugger and fails to start if it can't</li>
     *     <li>{@code suspend=y} (the default) - the JVM waits for a debugger before running any code</li>
     * </ul>
     * A debugger attached later to a {@code server=y,suspend=n} JVM is not detected.
     */
    static boolean isDebuggerAttached(List<String> jvmArguments) {
        for (String argument : jvmArguments) {
            String agentOptions;
            if (argument.startsWith("-agentlib:jdwp")) {
                agentOptions = argument.substring("-agentlib:jdwp".length());
            } else if (argument.startsWith("-Xrunjdwp")) {
                agentOptions = argument.substring("-Xrunjdwp".length());
            } else {
                continue;
            }

            String server = "n";
            String suspend = "y";
            for (String option : agentOptions.replaceFirst("^[=:]", "").split(",")) {
                if (option.startsWith("server=")) {
                    server = option.substring("server=".length());
                } else if (option.startsWith("suspend=")) {
                    suspend = option.substring("suspend=".length());
                }
            }

            if (!"y".equals(server) || !"n".equals(suspend)) {
                return true;
            }
        }

        return false;
    }

    protected void openBrowser(String url) {
        System.out.println(url);

        String osName = System.getProperty("os.name", "").toLowerCase();

        try {
            if (!osName.contains("win")) {
                if (osName.contains("mac") || osName.contains("darwin")) {
                    new ProcessBuilder("open", url).start();
                } else {
                    new ProcessBuilder("xdg-open", url).start();
                }
                return;
            }

            // the Studio url contains '&', which cmd.exe would treat as a command separator,
            // so both the window title and the url have to be passed pre-quoted
            new ProcessBuilder("cmd", "/c", "start", "\"Stop & look at Studio\"", "\"" + url + "\"").start();
        } catch (IOException e) {
            throw new RuntimeException("Unable to open a browser for url: " + url, e);
        }
    }

    @Override
    public void close() {
        if (_disposed) {
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

        if (_scopedServer != null && _scopedDocumentStore.isValueCreated()) {
            try {
                _scopedDocumentStore.getValue().close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        if (_scopedServer != null) {
            try {
                _scopedServer.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        InputStream databaseDumpFileStream = getDatabaseDumpFileStream();
        if (databaseDumpFileStream != null) {
            try {
                databaseDumpFileStream.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        _disposed = true;
        disposed = true;

        if (onDriverClosed != null) {
            onDriverClosed.accept(this);
        }

        EventHelper.invoke(new ArrayList<>(onDriverClosedListeners), this, EventArgs.EMPTY);

        if (exceptions.size() > 0) {
            throw aggregate(exceptions);
        }
    }

    private static RuntimeException aggregate(List<Exception> exceptions) {
        RuntimeException aggregated = new RuntimeException(exceptions.stream()
                .map(x -> x.toString()).collect(Collectors.joining(", ")), exceptions.get(0));

        for (int i = 1; i < exceptions.size(); i++) {
            aggregated.addSuppressed(exceptions.get(i));
        }

        return aggregated;
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

    private static TestServerOptions defaultServerOptions() {
        TestServerOptions options = new TestServerOptions();

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
            Operation operation = docStore.smuggler().forDatabase(database)
                    .importAsync(options, getDatabaseDumpFilePath());
            operation.waitForCompletion();
        } else if (getDatabaseDumpFileStream() != null) {
            Operation operation = docStore.smuggler().forDatabase(database)
                    .importAsync(options, getDatabaseDumpFileStream());
            operation.waitForCompletion();
        }
    }

    private static IDocumentStore runServer() {
        return startAndConnect(TEST_SERVER, ObjectUtils.firstNonNull(_globalServerOptions, serverOptions.get()));
    }

    private IDocumentStore createScopedDocumentStore() {
        return startAndConnect(_scopedServer,
                ObjectUtils.firstNonNull(_scopedServerOptions, _globalServerOptions, serverOptions.get()));
    }

    /**
     * Starts an embedded server with the settings every test server needs - isolated from the
     * shipped settings.json and running in memory - and connects a store to it.
     */
    private static IDocumentStore startAndConnect(EmbeddedServer server, ServerOptions options) {
        List<String> commandLineArgs = new ArrayList<>();
        commandLineArgs.add("-c");
        try {
            commandLineArgs.add(CommandLineArgumentEscaper.escapeSingleArg(getEmptySettingsFile().getAbsolutePath()));
        } catch (IOException e) {
            throw new RavenException("Unable to start server: " + e.getMessage(), e);
        }
        commandLineArgs.addAll(options.getCommandLineArgs());
        commandLineArgs.add("--RunInMemory=true");
        options.setCommandLineArgs(commandLineArgs);

        server.startServer(options);

        DocumentStore store = new DocumentStore(server.getServerUri(), null);

        store.initialize();

        return store;
    }

    /**
     * @deprecated use {@link TestServerOptions#useFiddler()}, which mirrors the C# test driver: it makes the
     * server reachable through the machine name, so Fiddler captures its traffic without any client
     * configuration. This method instead replaces the process-wide {@link RequestExecutor#configureHttpClient}
     * and will be removed in the next major version.
     */
    @Deprecated
    public void withFiddler() {
        RequestExecutor.configureHttpClient = builder -> {
            HttpHost proxy = new HttpHost("http", "127.0.0.1", 8888);
            builder.setRoutePlanner(new DefaultProxyRoutePlanner(proxy));
        };
    }
}
