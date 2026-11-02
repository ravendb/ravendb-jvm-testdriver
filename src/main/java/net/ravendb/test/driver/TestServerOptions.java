package net.ravendb.test.driver;

import net.ravendb.embedded.ServerOptions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Server options for the test driver. Unlike plain {@link ServerOptions}, a test server fails to start
 * when no valid license is available.
 */
public final class TestServerOptions extends ServerOptions {

    public TestServerOptions() {
        getLicensing().setThrowOnInvalidOrMissingLicense(true);
    }

    /**
     * Binds the server to the machine name instead of localhost, so Fiddler captures its traffic
     * without any client configuration.
     *
     * @return options of a server whose traffic Fiddler can capture
     */
    public static TestServerOptions useFiddler() {
        TestServerOptions options = new TestServerOptions();
        options.setServerUrl("http://" + getMachineName() + ":0");
        options.getCommandLineArgs().add("--Security.UnsecuredAccessAllowed=PrivateNetwork");
        return options;
    }

    /**
     * The equivalent of .NET's Environment.MachineName:
     * <ul>
     *     <li>Windows - the NetBIOS computer name, which is the COMPUTERNAME variable</li>
     *     <li>Linux / macOS - gethostname() up to the first '.', without a DNS lookup</li>
     * </ul>
     */
    static String getMachineName() {
        if (SystemUtils.IS_OS_WINDOWS) {
            String computerName = System.getenv("COMPUTERNAME");
            return StringUtils.isNotBlank(computerName) ? computerName : getHostNameFromDns();
        }

        return toShortHostName(getUnixHostName());
    }

    static String toShortHostName(String hostName) {
        int dot = hostName.indexOf('.');
        return dot < 0 ? hostName : hostName.substring(0, dot);
    }

    /**
     * Java has no gethostname(), so it is read from where the OS exposes it, without the DNS lookup
     * {@link InetAddress#getLocalHost()} makes (which can return an FQDN, or fail on an unresolvable name).
     */
    private static String getUnixHostName() {
        // Linux: the kernel hostname gethostname() returns
        try {
            Path procHostName = Paths.get("/proc/sys/kernel/hostname");
            if (Files.isReadable(procHostName)) {
                String hostName = new String(Files.readAllBytes(procHostName), StandardCharsets.UTF_8).trim();
                if (!hostName.isEmpty()) {
                    return hostName;
                }
            }
        } catch (IOException e) {
            // fall back to the hostname command
        }

        // macOS, or Linux without /proc: the hostname command prints gethostname()
        try {
            Process process = new ProcessBuilder("hostname").start();
            String hostName;
            try (InputStream output = process.getInputStream()) {
                hostName = IOUtils.toString(output, StandardCharsets.UTF_8).trim();
            }
            if (process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0 && !hostName.isEmpty()) {
                return hostName;
            }
        } catch (IOException e) {
            // fall back to DNS
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return getHostNameFromDns();
    }

    private static String getHostNameFromDns() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Unable to determine the machine name: " + e.getMessage(), e);
        }
    }
}
