package query;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.DoubleStream;

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
	public static void insertData(RepositoryConnection conn, String namespace, ValueFactory f, int x, int repoType)
			throws IOException, InterruptedException {
		
			double[] time = new double[x];
		
			if (x == 1) {

				IRI alice = f.createIRI(namespace, "alice");
				IRI bob = f.createIRI(namespace, "bob");
				IRI charlie = f.createIRI(namespace, "charlie");
								
				double timeStartNano = System.nanoTime();
				
				conn.add(alice, RDF.TYPE, FOAF.PERSON);
				conn.add(alice, FOAF.NAME, f.createLiteral("Alice"));
				conn.add(alice, FOAF.MBOX, f.createLiteral("mailto:alice@example.org"));
				conn.add(alice, FOAF.KNOWS, bob);
				conn.add(alice, FOAF.KNOWS, charlie);
				conn.add(alice, FOAF.KNOWS, f.createIRI(namespace, "snoopy"));

				conn.add(bob, RDF.TYPE, FOAF.PERSON);
				conn.add(bob, FOAF.NAME, f.createLiteral("Bob"));
				conn.add(bob, FOAF.MBOX, f.createLiteral("mailto:bob@example.org"));
				conn.add(bob, FOAF.KNOWS, charlie);
				conn.add(bob, FOAF.KNOWS, f.createIRI(namespace, "sally"));

				conn.add(charlie, RDF.TYPE, FOAF.PERSON);
				conn.add(charlie, FOAF.NAME, f.createLiteral("Charlie"));
				conn.add(charlie, FOAF.MBOX, f.createLiteral("mailto:charlie@example.org"));
				conn.add(charlie, FOAF.KNOWS, alice);
			
				double timeEndNano = System.nanoTime();
				double NanoInMs = (timeEndNano - timeStartNano) / 1000000;
				time[0] = NanoInMs; 
				
				double min = Double.MAX_VALUE;
				double max = Double.MIN_VALUE;
				
				for(double i : time) {
					if(i<min)
						min = i;
					if(i>max)
						max=i;
				}
				
				double average = NanoInMs / x;
				
				Arrays.sort(time);
				double median;
				if (time.length % 2 == 0)
					median = ((double)time[time.length/2] + (double)time[time.length/2 - 1])/2;
				else
					median = (double)time[time.length/2];
				
				double variance = 0.0;
				for(int i = 0; i < time.length; i++) {
					variance = variance + Math.pow((time[i]-average),2);
				}
				
				double sdev = 0.0;
				sdev = Math.sqrt(variance);
				
				double NanoInMsRound = Math.round(NanoInMs*100)/100.0;
				double minRound = Math.round(min*100)/100.0;
				double maxRound = Math.round(max*100)/100.0;
				double averageRound = Math.round(average*100)/100.0;
				double medianRound = Math.round(median*100)/100.0;
				double varianceRound = Math.round(variance*100)/100.0;
				double sdevRound = Math.round(sdev*100)/100.0;
				
				String tab = "";
				
				if (String.valueOf(NanoInMsRound).length() > 6) {
					tab = "\t";
				}
				else {
					tab = "\t\t";
				}
				
				
				
				switch (repoType) {
				case 1:
					System.out.println("____________________________________");
					System.out.println("NativeStore");
					System.out.println("Datensätze:\t|" + x);
					System.out.println("------------------------------------");
					System.out.println("\t\t|Gesamt (ms)\t|Minimum (ms)\t|Maximum (ms)\t|Durschnitt (ms)\t|Median (ms)\t|Varianz (ms)\t|Std-Abweichung (ms)");
					System.out.println("Schreibdauer:\t|" + NanoInMsRound + tab + "|" + minRound + "\t\t|" + maxRound + "\t\t|" + averageRound + "\t\t\t|" + medianRound + "\t\t|" + varianceRound + "\t\t|" + sdevRound);
					break;
				case 2:
					System.out.println("____________________________________");
					System.out.println("MemoryStore");
					System.out.println("Datensätze:\t|" + x);
					System.out.println("------------------------------------");
					System.out.println("\t\t|Gesamt (ms)\t|Minimum (ms)\t|Maximum (ms)\t|Durschnitt (ms)\t|Median (ms)\t|Varianz (ms)\t|Std-Abweichung (ms)");
					System.out.println("Schreibdauer:\t|" + NanoInMsRound + tab + "|" + minRound + "\t\t|" + maxRound + "\t\t|" + averageRound + "\t\t\t|" + medianRound + "\t\t|" + varianceRound + "\t\t|" + sdevRound);
					break;
				}
				
			
			} else {

				int y = 1;
				int z = 0;
				
				while (y <= x) {

					IRI aliceMultiple = f.createIRI(namespace, "alice" + y);
					IRI bobMultiple = f.createIRI(namespace, "bob" + y);
					IRI charlieMultiple = f.createIRI(namespace, "charlie" + y);
					
					double timeStartNanoSingle = System.nanoTime();

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
					
					double timeEndNanoSingle = System.nanoTime();
					double NanoInMsSingle = (timeEndNanoSingle - timeStartNanoSingle) / 1000000;										
					time[z] = NanoInMsSingle;
									
					z++;
				}
			
				double sumMs = DoubleStream.of(time).sum();
				
				double min = Double.MAX_VALUE;
				double max = Double.MIN_VALUE;
				
				for(double i : time) {
					if(i<min)
						min = i;
					if(i>max)
						max=i;
				}
				
				double average = sumMs / x;
				
				double variance = 0.0;
				for(int i = 0; i < time.length; i++) {
					variance = variance + Math.pow((time[i]-average),2);
				}
				
				variance = variance / x;
							
				double sdev = 0.0;
				sdev = Math.sqrt(variance);
				
				Arrays.sort(time);
				double median;
				if (time.length % 2 == 0)
					median = ((double)time[time.length/2] + (double)time[time.length/2 - 1])/2;
				else
					median = (double)time[time.length/2];
				
				double sumMsRound = Math.round(sumMs*100)/100.0;
				double minRound = Math.round(min*100)/100.0;
				double maxRound = Math.round(max*100)/100.0;
				double averageRound = Math.round(average*100)/100.0;
				double medianRound = Math.round(median*100)/100.0;
				double varianceRound = Math.round(variance*100)/100.0;
				double sdevRound = Math.round(sdev*100)/100.0;
								
				String tab = "";
				
				if (String.valueOf(sumMsRound).length() > 6) {
					tab = "\t";
				}
				else {
					tab = "\t\t";
				}
					
				
				switch (repoType) {
				case 1:
					System.out.println("____________________________________");
					System.out.println("NativeStore");
					System.out.println("Datensätze:\t|" + x);
					System.out.println("------------------------------------");
					System.out.println("\t\t|Gesamt (ms)\t|Minimum (ms)\t|Maximum (ms)\t|Durschnitt (ms)\t|Median (ms)\t|Varianz (ms)\t|Std-Abweichung (ms)");
					System.out.println("Schreibdauer:\t|" + sumMsRound + tab + "|" + minRound + "\t\t|" + maxRound + "\t\t|" + averageRound + "\t\t\t|" + medianRound + "\t\t|" + varianceRound + "\t\t|" + sdevRound);
					break;
				case 2:
					System.out.println("____________________________________");
					System.out.println("MemoryStore");
					System.out.println("Datensätze:\t|" + x);
					System.out.println("------------------------------------");
					System.out.println("\t\t|Gesamt (ms)\t|Minimum (ms)\t|Maximum (ms)\t|Durschnitt (ms)\t|Median (ms)\t|Varianz (ms)\t|Std-Abweichung (ms)");
					System.out.println("Schreibdauer:\t|" + sumMsRound + tab + "|" + minRound + "\t\t|" + maxRound + "\t\t|" + averageRound + "\t\t\t|" + medianRound + "\t\t|" + varianceRound + "\t\t|" + sdevRound);
					break;
				}
				
				z++;

			}

			write2File(conn, Select.selectAll4Write(conn, namespace), x, repoType);
	}


	/**
	 * Schreibt die zuvor eingefügten Datensätze in eine TURTLE-Datei.
	 * 
	 * @param conn			Verbindung zum Repository
	 * @param model			Abgefragten Datensätze
	 * @param x				Anzahl der Datensätze
	 * @throws IOException
	 * 
	 */
	private static void write2File(RepositoryConnection conn, Model model, int x, int repoType) throws IOException {
		
		String repoName = "";
		
		switch (repoType) {
		case 1: 
			repoName = "Native";
			break;
		case 2: 
			repoName = "Memory";
			break;
		}

		FileOutputStream out = new FileOutputStream(System.getProperty("user.dir") + "\\data\\data" + x + "_" + repoName + ".ttl");
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
