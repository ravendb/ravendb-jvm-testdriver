package net.ravendb.test.driver;

import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.session.IDocumentSession;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("WeakerAccess")
public class SecondTest extends RavenTestDriver {

    @Test
    public void anotherTest() {
        try (IDocumentStore store = getDocumentStore()) {
            try (IDocumentSession session = store.openSession()) {
                BasicTest.Person person = session.load(BasicTest.Person.class, "people/1");
                assertThat(person)
                        .isNull();
            }
        }
    }
}
