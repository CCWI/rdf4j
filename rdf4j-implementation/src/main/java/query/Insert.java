package query;

import java.io.FileOutputStream;
import java.io.IOException;
import java.text.NumberFormat;

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

/**
 * Ist für das Einfügen der Datensätze zuständig.
 * 
 * @author Alexander Fischer
 *
 */
public class Insert {

	/**
	 * Fügt die aus der setupConnection(int) übergebene Anzahl an Datensätzen ein.
	 * 
	 * @param conn 						Verbindung zum Repository
	 * @param namespace					Namespace für IRIs
	 * @param f							ValueFactory
	 * @param x							Anzahl Datensätze, die eingefügt werden sollen
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	public static void insertDataValueFactory(RepositoryConnection conn, String namespace, ValueFactory f, int x)
			throws IOException, InterruptedException {
		

		if (x == 1) {
			
			double timeStartNano = System.nanoTime();
			
			IRI alice = f.createIRI(namespace, "alice");
			conn.add(alice, RDF.TYPE, FOAF.PERSON);
			conn.add(alice, FOAF.NAME, f.createLiteral("Alice"));
			conn.add(alice, FOAF.MBOX, f.createLiteral("mailto:alice@example.org"));

			IRI bob = f.createIRI(namespace, "bob");
			IRI charlie = f.createIRI(namespace, "charlie");

			conn.add(alice, FOAF.KNOWS, bob);
			conn.add(alice, FOAF.KNOWS, charlie);
			conn.add(alice, FOAF.KNOWS, f.createIRI(namespace, "snoopy"));

			conn.add(bob, RDF.TYPE, FOAF.PERSON);
			conn.add(bob, FOAF.NAME, f.createLiteral("Bob"));
			conn.add(bob, FOAF.KNOWS, charlie);

			conn.add(charlie, RDF.TYPE, FOAF.PERSON);
			conn.add(charlie, FOAF.NAME, f.createLiteral("Charlie"));
			conn.add(charlie, FOAF.KNOWS, alice);
			
			double timeEndNano = System.nanoTime();
			double NanoInMS = (timeEndNano - timeStartNano) / 1000000;
			NumberFormat n = NumberFormat.getInstance();
			n.setMaximumFractionDigits(2);
			String NanoInMSRound = n.format(NanoInMS);
			System.out.println("____________________________________\n");
			System.out.println("Datensätze:\t|" + x);
			System.out.println("------------------------------------");
			System.out.println("Schreibdauer:\t|" + NanoInMSRound + " ms");
			
		} else {

			int y = 1;
			
			double timeStartNano = System.nanoTime();
			
			while (y <= x) {
				
				IRI aliceMultiple = f.createIRI(namespace, "alice" + y);
				IRI bobMultiple = f.createIRI(namespace, "bob" + y);
				IRI charlieMultiple = f.createIRI(namespace, "charlie" + y);

				conn.add(aliceMultiple, RDF.TYPE, FOAF.PERSON);
				conn.add(aliceMultiple, FOAF.NAME, f.createLiteral("Alice" + y));
				conn.add(aliceMultiple, FOAF.MBOX, f.createLiteral("mailto:alice" + y + "@example.org"));
				conn.add(aliceMultiple, FOAF.KNOWS, f.createIRI("http://example.org/bob"+y));
				conn.add(bobMultiple, FOAF.KNOWS, f.createIRI("http://example.org/charlie"+y));
				conn.add(aliceMultiple, FOAF.KNOWS, f.createIRI("http://example.org/snoopy"+y));
				
				conn.add(bobMultiple,  RDF.TYPE, FOAF.PERSON);
				conn.add(bobMultiple, FOAF.NAME, f.createLiteral("Bob"+y));
				conn.add(bobMultiple, FOAF.KNOWS, f.createIRI("http://example.org/charlie"+y));
				
				conn.add(charlieMultiple, RDF.TYPE, FOAF.PERSON);
				conn.add(charlieMultiple, FOAF.NAME, f.createLiteral("Charlie" + y));
				conn.add(charlieMultiple, FOAF.KNOWS, f.createIRI("http://example.org/alice"+y));
					
				y = y + 1;
				
			}
			
			double timeEndNano = System.nanoTime();
			double NanoInMS = (timeEndNano - timeStartNano) / 1000000;
			NumberFormat n = NumberFormat.getInstance();
			n.setMaximumFractionDigits(2);
			String NanoInMSRound = n.format(NanoInMS);
			System.out.println("____________________________________\n");
			System.out.println("Datensätze:\t|" + x);
			System.out.println("------------------------------------");
			System.out.println("Schreibdauer:\t|" + NanoInMSRound + " ms");

		}

		write2File(conn, Select.selectAllValueFactory4Write(conn, namespace), x);

	}


	/**
	 * Schreibt die zuvor eingefügten in eine TURTLE-Datei.
	 * 
	 * @param conn			Verbindung zum Repository
	 * @param model			Abgefragten Datensätze
	 * @param x				Anzahl der Datensätze
	 * @throws IOException
	 * 
	 */
	private static void write2File(RepositoryConnection conn, Model model, int x) throws IOException {
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
