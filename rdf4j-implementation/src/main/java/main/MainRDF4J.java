package main;

import java.io.File;
import java.io.IOException;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

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

		/** Aufruf für NativeStore */
//		setupConnectionNative(10);
//		setupConnectionNative(20);
//		setupConnectionNative(30);
//		setupConnectionNative(40);
//		setupConnectionNative(50);
//		setupConnectionNative(60);
//		setupConnectionNative(70);
//		setupConnectionNative(80);
//		setupConnectionNative(90);
//		setupConnectionNative(100);
		
		/** Aufruf für MemoryStore */
		setupConnectionMemory(10);
		setupConnectionMemory(20);
		setupConnectionMemory(30);
		setupConnectionMemory(40);
		setupConnectionMemory(50);
		setupConnectionMemory(60);
		setupConnectionMemory(70);
		setupConnectionMemory(80);
		setupConnectionMemory(90);
		setupConnectionMemory(100);

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
	public static void setupConnectionNative(int x) throws IOException, InterruptedException {
		
		Repository repoNative;
		File dataDirNative = new File(System.getProperty("user.dir") + "/data/native/" + x + "/");
		
		repoNative = new SailRepository(new NativeStore(dataDirNative));
		
		repoNative.initialize();
		
		String namespace = "http://example.org/";
		ValueFactory fNative = repoNative.getValueFactory();

		
		try (RepositoryConnection connNative = repoNative.getConnection()) {
			
			int repoType = 1;
			
			Insert.insertData(connNative, namespace, fNative, x, repoType);
			
			Select.selectAll(connNative, x, repoType);

			connNative.close();
			repoNative.shutDown();
		}
	}
	
	public static void setupConnectionMemory(int x) throws IOException, InterruptedException {
		
		Repository repoMemory;
		
		File dataDirMemory = new File(System.getProperty("user.dir") + "/data/memory/" + x + "/");
		repoMemory = new SailRepository(new MemoryStore(dataDirMemory));
		repoMemory.initialize();
		
		String namespace = "http://example.org/";
		ValueFactory fMemory = repoMemory.getValueFactory();
		
		try (RepositoryConnection connMemory = repoMemory.getConnection()) {
			
			int repoType = 2;
			
			Insert.insertData(connMemory, namespace, fMemory, x, repoType);
			
			Select.selectAll(connMemory, x, repoType);

			connMemory.close();
			repoMemory.shutDown();
		}
	}
}
