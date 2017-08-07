package query;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;


public class Insert {

	public static void main(String[] args) {
		
		// Erstellen von Repositories (MemoryStore = Memory Store)
		Repository repo = new SailRepository(new MemoryStore());
		repo.initialize();
		
		String namespace = "http://example.org/";
		ValueFactory f = repo.getValueFactory();
		IRI alice = f.createIRI(namespace,"alice");	
		IRI bob = f.createIRI(namespace,"bob");
		IRI charlie = f.createIRI(namespace,"charlie");	
		IRI snoopy = f.createIRI(namespace, "snoopy");		

	
		try (RepositoryConnection conn = repo.getConnection()) {
				
				conn.add(alice, RDF.TYPE, FOAF.PERSON);
				conn.add(alice, FOAF.NAME, f.createLiteral("Alice"));
				conn.add(alice, FOAF.MBOX, f.createLiteral("mailto:alice@example.org"));
				conn.add(alice, FOAF.KNOWS, bob);
				conn.add(alice, FOAF.KNOWS, charlie);
				conn.add(alice, FOAF.KNOWS, snoopy);
			
				conn.add(bob, RDF.TYPE, FOAF.PERSON);
				conn.add(bob,  FOAF.NAME, f.createLiteral("Bob"));
				conn.add(bob, FOAF.KNOWS, charlie);
			
				conn.add(charlie, RDF.TYPE, FOAF.PERSON);
				conn.add(charlie, FOAF.NAME, f.createLiteral("Charlie"));
				conn.add(charlie, FOAF.KNOWS, alice);
				
				RepositoryResult<Statement> statement = conn.getStatements(null, null, null);				
				Model model = QueryResults.asModel(statement);
				
				 //Vergabe der Prefix
				model.setNamespace("", namespace);
				model.setNamespace("foaf", FOAF.NAMESPACE);
				
				// Ausgabe
				Rio.write(model, System.out, RDFFormat.TURTLE);

		}
		
	}

	
}	
			
		


