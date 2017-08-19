package query;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.NumberFormat;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

/**
 * Ist für die Abfrage der Datensätze zuständig.
 * 
 * @author Alexander Fischer
 *
 */
public class Select {

	/**
	 * Fragt alle Datensätze, die zuvor in das Repository eingefügt worden sind ab und gibt diese für
	 * das Schreiben in die Datei zurück.
	 * 
	 * @param conn			Verbindung zum Repository
	 * @param namespace		Namespace für IRIs
	 * @return				alle abgefragten Datensätze in Form eines Models
	 * 
	 */
	public static Model selectAllValueFactory4Write(RepositoryConnection conn, String namespace) {

		RepositoryResult<Statement> statement = conn.getStatements(null, null, null);
		Model model = QueryResults.asModel(statement);

		// Vergabe der Prefix
		model.setNamespace("", namespace);
		model.setNamespace("foaf", FOAF.NAMESPACE);

		return model;
	}
	
	/**
	 * Liest alle Datensätze aus der zuvor erstellten Datei aus.
	 * 
	 * @param conn						Verbindung zum Repository
	 * @param x							Anzahl der Datensätze
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	public static void selectAllValueFactory( RepositoryConnection conn, int x) throws IOException, InterruptedException {
		
		String data = System.getProperty("user.dir") + "\\data\\data" + x + ".ttl";
		FileInputStream input = new FileInputStream(data);
		RDFParser parser = Rio.createParser(RDFFormat.TURTLE);

		try {
			
			parser.parse(input, data);

			String qry = "prefix foaf: <http://xmlns.com/foaf/0.1/> \n"
					+ "SELECT ?person ?name ?email ?knows \n" + "WHERE { ?person foaf:name ?name . \n"
					+ "OPTIONAL { ?person foaf:mbox ?email . } \n "
					+ "OPTIONAL { ?person foaf:knows ?knows . } } ";
			
			double timeStartNano = System.nanoTime();
			
			conn.prepareTupleQuery(QueryLanguage.SPARQL, qry).evaluate();
			
			double timeEndNano = System.nanoTime();
			double NanoInMS = (timeEndNano - timeStartNano) / 1000000;
			 
			NumberFormat n = NumberFormat.getInstance();
			n.setMaximumFractionDigits(2);
	 
			String NanoInMSRound = n.format(NanoInMS);
			System.out.println("Lesedauer:\t|" + NanoInMSRound + " ms");
			System.out.println("____________________________________\n");
			

		} 
		catch (IOException e) {
			System.out.println(e);
		}
		catch (RDFParseException e) {
			System.out.println(e);
		}
		catch (RDFHandlerException e) {
			System.out.println(e);
		} 
		finally {
			input.close();
		}
	}

}
