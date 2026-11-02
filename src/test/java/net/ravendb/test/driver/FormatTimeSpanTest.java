package net.ravendb.test.driver;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Expected values are what .NET's TimeSpan.ToString() returns for the same duration.
 */
public class FormatTimeSpanTest {

    @Test
    public void wholeSeconds() {
        assertThat(RavenTestDriver.formatTimeSpan(Duration.ofMinutes(1))).isEqualTo("00:01:00");
        assertThat(RavenTestDriver.formatTimeSpan(Duration.ofSeconds(30))).isEqualTo("00:00:30");
        assertThat(RavenTestDriver.formatTimeSpan(Duration.ZERO)).isEqualTo("00:00:00");
    }

    @Test
    public void fractionIsPrintedAsSevenDigitTicks() {
        assertThat(RavenTestDriver.formatTimeSpan(Duration.ofMillis(150))).isEqualTo("00:00:00.1500000");
        assertThat(RavenTestDriver.formatTimeSpan(Duration.ofMillis(10_500))).isEqualTo("00:00:10.5000000");
    }

    @Test
    public void daysArePrefixed() {
        assertThat(RavenTestDriver.formatTimeSpan(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)))
                .isEqualTo("1.02:03:04");
    }

    @Test
    public void negativeDurationIsSigned() {
        assertThat(RavenTestDriver.formatTimeSpan(Duration.ofMinutes(-1))).isEqualTo("-00:01:00");
    }
}
