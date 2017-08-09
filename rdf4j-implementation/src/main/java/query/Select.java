package query;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

public class Select {

	public static Model selectAll(RepositoryConnection conn, String namespace) {

		RepositoryResult<Statement> statement = conn.getStatements(null, null, null);
		Model model = QueryResults.asModel(statement);

		// Vergabe der Prefix
		model.setNamespace("", namespace);
		model.setNamespace("foaf", FOAF.NAMESPACE);

		// System.out.println("Ausgabe beginnt\n");
		// System.out.println("-----------------------------------");
		// Ausgabe
		// Rio.write(model, System.out, RDFFormat.TURTLE);

		// conn.close();
		// System.out.println("-----------------------------------");
		// System.out.println("\nAusgabe beendet\n");

		return model;
	}

	public static void selectAllSPARQL(Repository repo, RepositoryConnection conn, int x) throws IOException {
		long begin = new Date().getTime();

		// System.out.println(begin);
		String data = System.getProperty("user.dir") + "\\data\\data" + x + ".ttl";
		FileInputStream input = new FileInputStream(data);
		RDFParser parser = Rio.createParser(RDFFormat.TURTLE);

		try {
			parser.parse(input, data);
			// String qry = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>" +
			// "SELECT ?name (COUNT(?friend) AS ?count) " +
			// "WHERE { ?x foaf:name ?name ." +
			// " ?x foaf:knows ?friend ." +
			// "} GROUP BY ?person ?name";

			String qry = "prefix foaf: <http://xmlns.com/foaf/0.1/> \n"
					+ "SELECT ?person ?name ?email ?knows ?firstName \n" + "WHERE { ?person foaf:name ?name . \n"
					+ "  	     OPTIONAL { ?person foaf:mbox ?email . } \n "
					+ "		 OPTIONAL { ?person foaf:knows ?knows . } \n "
					+ "		 OPTIONAL { ?person foaf:firstName ?firstName } \n" + "     }";

			// System.out.println("Abfrage:");
			// System.out.println(qry + "\n");

			TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, qry);
			TupleQueryResult results = tq.evaluate();

			// System.out.println("Ausgabe:");
			// System.out.println("\tIRI\t\t\t\tName\t\tE-Mail\t\t\t\tkennt");

			int row = 1;

			while (results.hasNext()) {
				BindingSet s = results.next();

				String person;
				String name;
				String mbox;
				String knows;
				String ausgabe;

				if (s.getValue("person") == null) {
					ausgabe = "\t\t\t\t";

				} else {
					person = s.getValue("person").stringValue();
					ausgabe = person + "  \t";
				}

				if (s.getValue("name") == null) {
					ausgabe = ausgabe + "\t\t";
				} else {
					name = s.getValue("name").stringValue();
					ausgabe = ausgabe + name + "\t\t";
				}

				if (s.getValue("email") == null) {
					ausgabe = ausgabe + "\t\t\t\t";
				} else {
					mbox = s.getValue("email").stringValue();
					ausgabe = ausgabe + mbox + "\t";
				}

				if (s.getValue("knows") == null) {
					ausgabe = ausgabe + "\t\t";
				} else {
					knows = s.getValue("knows").stringValue();
					ausgabe = ausgabe + knows + "  \t";
				}

				// System.out.println(row + "\t" + ausgabe + "\n");

				row = row + 1;

			}

			long end = new Date().getTime();
			// System.out.println(end);

			long DifTimeMs = end - begin;
			double DifTimeS = DifTimeMs / 1000.00;

			System.out.println("Abfrage von " + x + " Datensatz/-sätzen \n" + "Dauer: " + DifTimeMs + " ms --> "
					+ DifTimeS + " s" + "\n");

		} catch (IOException e) {
			System.out.println(e);
		}

		catch (RDFParseException e) {
			System.out.println(e);
		}

		catch (RDFHandlerException e) {
			System.out.println(e);
		} finally {
			input.close();
		}
	}

}
