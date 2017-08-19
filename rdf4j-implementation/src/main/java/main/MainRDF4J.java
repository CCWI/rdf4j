package main;

import java.io.IOException;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import query.Insert;
import query.Select;

/**
 *
 * Schafft die Voraussetzungen für die Speicherung und die Abfrage der Daten.
 * 
 * @author Alexander Fischer
 *
 */
public class MainRDF4J {

	/**
	 * Ruft die Funktion setupConnection(int) mit der Anzahl der zu erstellenden und abzufragenden Datensätze auf. 
	 * 
	 * @param  args
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {

		setupConnection(1);
		setupConnection(10);
		setupConnection(100);
		setupConnection(200);		
		setupConnection(300);		
		setupConnection(400);		
		setupConnection(500);		
		setupConnection(600);
		setupConnection(700);
		setupConnection(800);
		setupConnection(900);
		setupConnection(1000);
		setupConnection(2000);
		setupConnection(3000);
		setupConnection(4000);
		setupConnection(5000);
		setupConnection(6000);
		setupConnection(7000);
		setupConnection(8000);
		setupConnection(9000);
		setupConnection(10000);
		setupConnection(20000);
		setupConnection(30000);
		setupConnection(40000);
		setupConnection(50000);
		setupConnection(60000);
		setupConnection(70000);
		setupConnection(80000);
		setupConnection(90000);
		setupConnection(100000);

	}

	/**
	 * Erstellt das Repository, initialisiert dieses und baut die Verbindung dazu auf, um 
	 * die Datensätze in die ebenfalls erstellte ValueFactory einzufügen und abzufragen. 
	 * 
	 * @param  x 					Anzahl an Datensätzen
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	public static void setupConnection(int x) throws IOException, InterruptedException {
		
		Repository repo;
		repo = new SailRepository(new MemoryStore());
		repo.initialize();

		String namespace = "http://example.org/";
		ValueFactory f = repo.getValueFactory();
		
		try (RepositoryConnection conn = repo.getConnection()) {
			
			Insert.insertDataValueFactory(conn, namespace, f, x);
			
			//Thread.sleep(2000);
			
			Select.selectAllValueFactory(conn, x);

			conn.close();
			repo.shutDown();

		}
	}
}
