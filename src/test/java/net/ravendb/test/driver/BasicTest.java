package net.ravendb.test.driver;

import net.ravendb.client.documents.IDocumentStore;
import net.ravendb.client.documents.session.IDocumentSession;
import org.junit.jupiter.api.Test;

@SuppressWarnings("WeakerAccess")
public class BasicTest extends RavenTestDriver {

    @Test
    public void test() {
        try (IDocumentStore store = getDocumentStore()) {
            try (IDocumentSession session = store.openSession()) {
                Person person = new Person();
                person.setName("John");

                session.store(person, "people/1");
                session.saveChanges();
            }
        }
    }

    @Test
    public void test2() {
        try (IDocumentStore store = getDocumentStore()) {
            try (IDocumentSession session = store.openSession()) {
                Person person = new Person();
                person.setName("Grisha");

                session.store(person, "people/1");
                session.saveChanges();
            }
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class Person {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
