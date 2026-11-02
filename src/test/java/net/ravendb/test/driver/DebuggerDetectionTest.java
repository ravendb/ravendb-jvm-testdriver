package net.ravendb.test.driver;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class DebuggerDetectionTest {

    @Test
    public void noDebugAgentMeansNoDebugger() {
        assertThat(RavenTestDriver.isDebuggerAttached(Collections.emptyList())).isFalse();
        assertThat(RavenTestDriver.isDebuggerAttached(Arrays.asList("-Xmx1g", "-Dfile.encoding=UTF-8"))).isFalse();
    }

    @Test
    public void ideLaunchedDebugSessionsAreDetected() {
        // IntelliJ
        assertThat(RavenTestDriver.isDebuggerAttached(Collections.singletonList(
                "-agentlib:jdwp=transport=dt_socket,address=127.0.0.1:52345,suspend=y,server=n"))).isTrue();
        // Eclipse - server and suspend left at their defaults
        assertThat(RavenTestDriver.isDebuggerAttached(Collections.singletonList(
                "-agentlib:jdwp=transport=dt_socket,address=localhost:52345"))).isTrue();
        // legacy syntax
        assertThat(RavenTestDriver.isDebuggerAttached(Arrays.asList(
                "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=n,address=8000"))).isTrue();
    }

    @Test
    public void listeningAgentThatWaitsForTheDebuggerIsDetected() {
        assertThat(RavenTestDriver.isDebuggerAttached(Collections.singletonList(
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"))).isTrue();
    }

    @Test
    public void listeningAgentThatDoesNotWaitIsNotADebugger() {
        assertThat(RavenTestDriver.isDebuggerAttached(Collections.singletonList(
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"))).isFalse();
        assertThat(RavenTestDriver.isDebuggerAttached(Collections.singletonList(
                "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"))).isFalse();
    }
}
