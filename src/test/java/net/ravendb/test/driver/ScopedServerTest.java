package net.ravendb.test.driver;

import com.google.common.io.Files;
import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.primitives.CleanCloseable;
import net.ravendb.embedded.EmbeddedServer;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Every test here starts its own embedded server, alongside the JVM-wide one the other tests share.
 * The licensing tests are the JVM counterpart of C#'s LicenseOptionsTestDriverTests, limited to the
 * server-option scenarios: the JVM cannot change the environment the server process inherits.
 */
public class ScopedServerTest extends RavenTestDriver {

    private static final String INVALID_LICENSE = "SomeInvalidLicense";
    private static final String LICENSE_MISSING = "The RavenDB server cannot start due to a missing license";

    private final List<File> tempDirs = new ArrayList<>();

    @AfterEach
    public void deleteTempDirs() {
        tempDirs.forEach(FileUtils::deleteQuietly);
    }

    @Test
    public void scopedServerIsIndependentFromTheGlobalOne() {
        try (IDocumentStore globalStore = getDocumentStore();
             RavenTestDriver driver = new RavenTestDriver();
             CleanCloseable ignored = driver.configureScopedServer(scopedOptions());
             IDocumentStore scopedStore = driver.getDocumentStore()) {

            assertThat(scopedStore.getUrls()).isNotEqualTo(globalStore.getUrls());
            assertThat(scopedStore.getDatabase()).startsWith("scopedServerIsIndependentFromTheGlobalOne_");
        }
    }

    @Test
    public void closingTheScopedHandleReturnsTheDriverToTheGlobalServer() {
        try (IDocumentStore globalStore = getDocumentStore();
             RavenTestDriver driver = new RavenTestDriver()) {

            CleanCloseable scope = driver.configureScopedServer(scopedOptions());
            try (IDocumentStore scopedStore = driver.getDocumentStore()) {
                assertThat(scopedStore.getUrls()).isNotEqualTo(globalStore.getUrls());
            }
            scope.close();

            try (IDocumentStore store = driver.getDocumentStore()) {
                assertThat(store.getUrls()).isEqualTo(globalStore.getUrls());
            }

            assertThatThrownBy(() -> driver.configureScopedServer(scopedOptions()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Cannot set embedded server after document store has been initialized.");
        }
    }

    @Test
    public void scopedServerCanBeConfiguredOnlyOnce() {
        try (RavenTestDriver driver = new RavenTestDriver();
             CleanCloseable ignored = driver.configureScopedServer(scopedOptions())) {

            assertThatThrownBy(() -> driver.configureScopedServer(scopedOptions()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Local embedded server is already set.");
        }
    }

    @Test
    public void closingTheDriverStopsTheScopedServer() {
        EmbeddedServer server = new EmbeddedServer();

        RavenTestDriver driver = new RavenTestDriver();
        driver.configureScopedServer(scopedOptions(), server);
        driver.getDocumentStore();
        assertThat(server.getServerUri()).isNotBlank();

        driver.close();

        assertThatThrownBy(server::getServerUri)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void scopedServersSharingTheServerLocationRunSideBySide() {
        // the default TargetServerLocation is left untouched - every live server must still get its own
        // copy of the binaries, or extracting into the folder of a running server fails on locked files
        TestServerOptions options = new TestServerOptions();
        options.setDataDirectory(tempDir().getAbsolutePath());
        options.setLogsPath(tempDir().getAbsolutePath());
        String sharedLocation = options.getTargetServerLocation();

        try (RavenTestDriver first = new RavenTestDriver();
             CleanCloseable firstScope = first.configureScopedServer(options);
             IDocumentStore firstStore = first.getDocumentStore();
             RavenTestDriver second = new RavenTestDriver();
             CleanCloseable secondScope = second.configureScopedServer(options);
             IDocumentStore secondStore = second.getDocumentStore()) {

            assertThat(secondStore.getUrls()).isNotEqualTo(firstStore.getUrls());
        }

        assertThat(options.getTargetServerLocation())
                .as("the caller's options must keep their server location")
                .isEqualTo(sharedLocation);
    }

    @Test
    public void scopedServerLocationIsDeletedOnClose() {
        try (RavenTestDriver driver = new RavenTestDriver()) {
            CleanCloseable scope = driver.configureScopedServer(scopedOptions());
            driver.getDocumentStore().close();
            File location = driver.getScopedServerLocation();
            assertThat(location).isDirectory();

            scope.close();

            assertThat(location).doesNotExist();
        }
    }

    @Test
    public void reusedOptionsDoNotRepeatTheTestArguments() {
        TestServerOptions options = scopedOptions();

        try (RavenTestDriver first = new RavenTestDriver();
             CleanCloseable firstScope = first.configureScopedServer(options);
             IDocumentStore firstStore = first.getDocumentStore();
             RavenTestDriver second = new RavenTestDriver();
             CleanCloseable secondScope = second.configureScopedServer(options);
             IDocumentStore secondStore = second.getDocumentStore()) {

            assertThat(options.getCommandLineArgs()).containsOnlyOnce("-c", "--RunInMemory=true");
        }
    }

    @Test
    public void failedScopedStartIsCachedInsteadOfRetried() {
        TestServerOptions options = scopedOptions();
        options.getLicensing().setLicense(INVALID_LICENSE);

        try (RavenTestDriver driver = new RavenTestDriver();
             CleanCloseable ignored = driver.configureScopedServer(options)) {

            Throwable first = catchThrowable(driver::getDocumentStore);
            Throwable second = catchThrowable(driver::getDocumentStore);

            assertThat(first).isNotNull();
            assertThat(second)
                    .as("the original startup failure must be rethrown, not 'The server was already started'")
                    .isSameAs(first);
        }
    }

    @Test
    public void license_enforcedByDefault_invalidLicense_shouldThrow() {
        TestServerOptions options = scopedOptions();
        options.getLicensing().setLicense(INVALID_LICENSE);

        assertThat(catchThrowable(() -> startScoped(options)))
                .isNotNull()
                .hasStackTraceContaining(LICENSE_MISSING);
    }

    @Test
    public void license_enforceTrue_invalidLicense_shouldThrow() {
        TestServerOptions options = scopedOptions();
        options.getLicensing().setThrowOnInvalidOrMissingLicense(true);
        options.getLicensing().setLicense(INVALID_LICENSE);

        assertThat(catchThrowable(() -> startScoped(options)))
                .isNotNull()
                .hasStackTraceContaining(LICENSE_MISSING);
    }

    @Test
    public void license_enforceFalse_invalidLicense_shouldWork() {
        TestServerOptions options = scopedOptions();
        options.getLicensing().setThrowOnInvalidOrMissingLicense(false);
        options.getLicensing().setLicense(INVALID_LICENSE);

        startScoped(options);
    }

    @Test
    public void license_enforceFalse_invalidLicensePath_shouldWork() throws Exception {
        TestServerOptions options = scopedOptions();
        options.getLicensing().setThrowOnInvalidOrMissingLicense(false);
        options.getLicensing().setLicensePath(licenseFile(INVALID_LICENSE).getAbsolutePath());

        startScoped(options);
    }

    @Test
    public void license_enforceTrue_validLicense_shouldWork() {
        String validLicense = validLicense();

        TestServerOptions options = scopedOptions();
        options.getLicensing().setLicense(validLicense);

        startScoped(options);
    }

    @Test
    public void license_enforceTrue_validLicensePath_shouldWork() throws Exception {
        String validLicense = validLicense();

        TestServerOptions options = scopedOptions();
        options.getLicensing().setLicensePath(licenseFile(validLicense).getAbsolutePath());

        startScoped(options);
    }

    @Test
    public void license_enforceTrue_invalidLicensePath_shouldThrow() throws Exception {
        // the server only falls back to License.Path when the License key is not set,
        // and the License key is otherwise inherited from the environment
        assumeTrue(System.getenv("RAVEN_License") == null, "a license is set in the environment");

        TestServerOptions options = scopedOptions();
        options.getLicensing().setLicensePath(licenseFile(INVALID_LICENSE).getAbsolutePath());

        assertThat(catchThrowable(() -> startScoped(options)))
                .isNotNull()
                .hasStackTraceContaining(LICENSE_MISSING);
    }

    @Test
    public void useFiddlerServesThroughTheMachineName() {
        TestServerOptions options = withTempDirs(TestServerOptions.useFiddler());

        try (RavenTestDriver driver = new RavenTestDriver();
             CleanCloseable ignored = driver.configureScopedServer(options);
             IDocumentStore store = driver.getDocumentStore()) {

            assertThat(store.getUrls()[0].toLowerCase())
                    .startsWith("http://" + TestServerOptions.getMachineName().toLowerCase() + ":");
        }
    }

    private void startScoped(TestServerOptions options) {
        try (RavenTestDriver driver = new RavenTestDriver();
             CleanCloseable ignored = driver.configureScopedServer(options);
             IDocumentStore store = driver.getDocumentStore()) {
            assertThat(store.getDatabase()).isNotBlank();
        }
    }

    private static String validLicense() {
        String license = System.getenv("RAVEN_License");
        assumeTrue(license != null && !license.trim().isEmpty(), "RAVEN_License is not set");
        return license;
    }

    private File licenseFile(String content) throws Exception {
        File dir = tempDir();
        File file = new File(dir, "license.json");
        FileUtils.write(file, content, StandardCharsets.UTF_8);
        return file;
    }

    private TestServerOptions scopedOptions() {
        return withTempDirs(new TestServerOptions());
    }

    private TestServerOptions withTempDirs(TestServerOptions options) {
        options.setTargetServerLocation(tempDir().getAbsolutePath());
        options.setDataDirectory(tempDir().getAbsolutePath());
        options.setLogsPath(tempDir().getAbsolutePath());
        return options;
    }

    private File tempDir() {
        File dir = Files.createTempDir();
        tempDirs.add(dir);
        return dir;
    }
}
