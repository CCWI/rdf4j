package query;

import java.io.FileOutputStream;
import java.io.IOException;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

public class Insert {

	public static int insertData(RepositoryConnection conn, String namespace, ValueFactory f, int x)
			throws IOException {

		// System.out.println(x);

		if (x == 1) {

			IRI alice = f.createIRI(namespace, "alice");
			conn.add(alice, RDF.TYPE, FOAF.PERSON);
			conn.add(alice, FOAF.NAME, f.createLiteral("Alice"));
			conn.add(alice, FOAF.MBOX, f.createLiteral("mailto:alice@example.org"));

			IRI bob = f.createIRI(namespace, "bob");
			IRI charlie = f.createIRI(namespace, "charlie");
			IRI snoopy = f.createIRI(namespace, "snoopy");

			conn.add(alice, FOAF.KNOWS, bob);
			conn.add(alice, FOAF.KNOWS, charlie);
			conn.add(alice, FOAF.KNOWS, snoopy);

			conn.add(bob, RDF.TYPE, FOAF.PERSON);
			conn.add(bob, FOAF.NAME, f.createLiteral("Bob"));
			conn.add(bob, FOAF.KNOWS, charlie);

			conn.add(charlie, RDF.TYPE, FOAF.PERSON);
			conn.add(charlie, FOAF.NAME, f.createLiteral("Charlie"));
			conn.add(charlie, FOAF.KNOWS, alice);
		} else {

			int y = 1;
			// System.out.println("x ist nicht 1");

			while (y <= x) {

				IRI aliceMultiple = f.createIRI(namespace, "alice" + y);

				conn.add(aliceMultiple, RDF.TYPE, FOAF.PERSON);
				conn.add(aliceMultiple, FOAF.NAME, f.createLiteral("Alice" + y));
				conn.add(aliceMultiple, FOAF.MBOX, f.createLiteral("mailto:alice" + y + "@example.org"));
				conn.add(aliceMultiple, FOAF.KNOWS, f.createIRI("http://example.org/alice"));

				y = y + 1;
			}
		}

		write2File(conn, Select.selectAll(conn, namespace), f, x);

		return x;

	}

	private static void write2File(RepositoryConnection conn, Model model, ValueFactory f, int x) throws IOException {
		FileOutputStream out = new FileOutputStream(System.getProperty("user.dir") + "\\data\\data" + x + ".ttl");
		RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, out);

		try {
			writer.startRDF();
			for (Statement st : model) {
				writer.handleStatement(st);
			}
			writer.endRDF();
		} finally {
			out.close();
		}
	}

}
