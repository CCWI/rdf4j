package main;

import java.io.IOException;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import query.Insert;
import query.Select;

public class MainRDF4J {

	public static void main(String[] args) throws IOException {

		setupConnection(1);

		setupConnection(10);

		setupConnection(1000);

		setupConnection(10000);

		setupConnection(100000);

		// setupConnection(100000000);

		// System.out.println("Abfrage beendet!");

	}

	public static void setupConnection(int x) throws IOException {

		Repository repo;
		repo = new SailRepository(new MemoryStore());
		repo.initialize();

		String namespace = "http://example.org/";
		ValueFactory f = repo.getValueFactory();

		try (RepositoryConnection conn = repo.getConnection()) {
			Select.selectAllSPARQL(repo, conn, Insert.insertData(conn, namespace, f, x));
			conn.close();
			repo.shutDown();

		}
	}
}
