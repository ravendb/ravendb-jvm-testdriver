package net.ravendb.test.driver;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestServerOptionsTest {

    @Test
    public void licenseIsEnforcedByDefault() {
        assertThat(new TestServerOptions().getLicensing().isThrowOnInvalidOrMissingLicense())
                .isTrue();
    }

    @Test
    public void useFiddlerBindsTheServerToTheMachineName() {
        TestServerOptions options = TestServerOptions.useFiddler();

        assertThat(options.getServerUrl())
                .isEqualTo("http://" + TestServerOptions.getMachineName() + ":0");
        assertThat(options.getCommandLineArgs())
                .containsExactly("--Security.UnsecuredAccessAllowed=PrivateNetwork");
        assertThat(options.getLicensing().isThrowOnInvalidOrMissingLicense())
                .isTrue();
    }

    @Test
    public void useFiddlerReturnsAFreshInstanceEveryTime() {
        TestServerOptions first = TestServerOptions.useFiddler();
        TestServerOptions second = TestServerOptions.useFiddler();

        first.getCommandLineArgs().add("--extra");

        assertThat(second).isNotSameAs(first);
        assertThat(second.getCommandLineArgs())
                .containsExactly("--Security.UnsecuredAccessAllowed=PrivateNetwork");
    }

    @Test
    public void machineNameIsResolved() {
        assertThat(TestServerOptions.getMachineName()).isNotBlank();
    }

    @Test
    public void machineNameIsTheShortHostName() {
        // like .NET's Environment.MachineName, never an FQDN
        assertThat(TestServerOptions.getMachineName()).doesNotContain(".");
    }

    @Test
    public void hostNameIsTruncatedAtTheFirstDot() {
        assertThat(TestServerOptions.toShortHostName("myhost.corp.local")).isEqualTo("myhost");
        assertThat(TestServerOptions.toShortHostName("myhost")).isEqualTo("myhost");
    }
}
