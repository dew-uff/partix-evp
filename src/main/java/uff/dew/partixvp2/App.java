package uff.dew.partixvp2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import mpi.MPI;
import mpi.MPIException;
import uff.dew.svp.Partitioner;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) throws MPIException {
        
        int myrank = -1;
        List<String> fragments = null;
        
        // **************************** INIT **********************************
        
        String[] myargs = MPI.Init(args);       
        
        // 0 nthreads per node
        // 1 nnodes
        // 2 shared dir
        // 3 nfragments
        // 4 db type
        // 5 db name
        // 6 db catalog
        // 7 query

        if (myargs.length < 8) {
            System.err.println("Not enough arguments!");
            MPI.Finalize();
            System.exit(1);
        }
        
        myrank = MPI.COMM_WORLD.Rank();

        int THREADS_PER_NODE = -1;   
        int NUMBER_NODES = -1;
        int TOTAL_NUMBER_THREADS = -1;
        String sharedDir = null;
        int N_FRAGMENTS = -1;
        String DB_TYPE = null;
        String DB_NAME = null;
        String DB_CATALOG_FILE = null;
        String QUERY_FILE = null;
        
        // get configuration
        try {
            THREADS_PER_NODE = Integer.parseInt(myargs[0]);   
            NUMBER_NODES = Integer.parseInt(myargs[1]);
            TOTAL_NUMBER_THREADS = THREADS_PER_NODE * NUMBER_NODES; 
            sharedDir = myargs[2];
            N_FRAGMENTS = Integer.parseInt(myargs[3]);
            DB_TYPE = myargs[4];
            DB_NAME = myargs[5];
            DB_CATALOG_FILE = myargs[6];
            QUERY_FILE = myargs[7];
        }
        catch (Exception e) {
            System.err.println("Error reading config parameters!");
            MPI.Finalize();
            System.exit(1);
        }
        
        final int DB_PORT = DB_TYPE.equals("BASEX")?1984:5050;
        final String DB_USER = DB_TYPE.equals("BASEX")?"admin":"SYSTEM";
        final String DB_PASSWORD = DB_TYPE.equals("BASEX")?"admin":"MANAGER";
        
        Thread[] th = new Thread[THREADS_PER_NODE];

        // **************************** PARTITIONING **********************************
        
        if (myrank == 0) {
            
            System.out.println("PartiX-VPv2");
            System.out.println("Date: " + new Date());
            System.out.println("Threads per node: " + THREADS_PER_NODE);
            System.out.println("# of nodes: " + NUMBER_NODES);
            System.out.println("Total # of processors: " + TOTAL_NUMBER_THREADS);
            System.out.println("Shared dir: " + sharedDir);
            System.out.println("Database: " + DB_NAME);
            System.out.println("Database type: " + DB_TYPE);
            System.out.println("Num fragments: " + N_FRAGMENTS);
            System.out.println("------");
            
            // query file
            String query = null;
            try {
                query = readContentFromFile(QUERY_FILE);
                verifyQuery(query);
            } catch (FileNotFoundException e) {
                System.err.println("Query file was not found!");
                MPI.Finalize();
                System.exit(1);
            } catch (IOException e) {
                System.err.println("Something went wrong while reading query file!");
                MPI.Finalize();
                System.exit(1);
            } catch (Exception e) {
                System.err.println("Query not supported or wrong: " + e.getMessage());
                MPI.Finalize();
                System.exit(1);
            }
            
            System.out.print("Performing virtual partitioning over query... ");
            
            
            try {
                long timestamp = System.currentTimeMillis();

                Partitioner partitioner = null;
                
                if (DB_CATALOG_FILE != null) {
                    FileInputStream fis = new FileInputStream(DB_CATALOG_FILE);
                    partitioner = new Partitioner(fis);
                    fis.close();
                } 
                else {
                    partitioner = new Partitioner("locahost", DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_TYPE);
                }
                
                fragments = partitioner.executePartitioning(query, N_FRAGMENTS);
                
                long partitioningMilis = System.currentTimeMillis() - timestamp;
                
                System.out.println(" done");
                
                System.out.println("Partitioning time: " + partitioningMilis + " ms.");

            } catch (Exception e) {
                System.err.println("Something wrong executing the partitioner: " + e.getMessage());
                e.printStackTrace();
                MPI.Finalize();
                System.exit(1);
            }
            
            th[0] = new Thread(new Mediator(fragments));
            for (int i = 1; i < THREADS_PER_NODE; i++) {
                int id = THREADS_PER_NODE*myrank + i;
                th[i] = new Thread(new Worker(id, sharedDir, "localhost", DB_PORT, 
                        DB_USER, DB_PASSWORD, DB_NAME, DB_TYPE));
            }
        }
        else {
            for (int i = 0; i < THREADS_PER_NODE; i++) {
                int id = THREADS_PER_NODE*myrank + i;
                th[i] = new Thread(new Worker(id, sharedDir, "localhost", DB_PORT, 
                        DB_USER, DB_PASSWORD, DB_NAME, DB_TYPE));                
            }
        }
        
        if (myrank == 0) {
            System.out.print("Syncing all nodes... ");
        }
        
        MPI.COMM_WORLD.Barrier();
        
        if (myrank == 0) {
            System.out.println("done!");
        }

        // **************************** PARALLEL EXECUTION *******************************
        
        long timestamp = 0;
        
        if (myrank == 0) {
            timestamp = System.currentTimeMillis();  
        }

        for (int i = 0; i < THREADS_PER_NODE; i++) {
            th[i].start();          
        }
            
        for (int i = 0; i < THREADS_PER_NODE; i++) {
            // Aguarda até que todas as threads sejam finalizadas.
            try {
                th[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }           
                
        MPI.COMM_WORLD.Barrier(); // Aguarda até que todos os nós tenham finalizado seus jobs. 
                
        if (myrank==0) { // Nó 0 é o nó de controle, responsável pela consolidação dos resultados.          
            long parallelProcessingTime = (System.currentTimeMillis() - timestamp); // Calcula o tempo de execução de todas as sub-consultas. Tempo retornado em milisegundos.      
            System.out.println("Subquery phase execution time: " + parallelProcessingTime);
            
//            try {
//                long t1 = System.nanoTime();
//                // caminho onde será salvo o documento com a resposta final
//                String completeFileName = sharedDir + "/finalResult/xqueryAnswer.xml";
//
//                File file = new File(completeFileName);     
//                FileOutputStream out = new FileOutputStream(file);
//                
//                FinalResultComposer frc = new FinalResultComposer(out);
//                frc.setDatabaseInfo("localhost", 1984, "admin", "admin", "expdb", "PURE_BASEX");
//                
//                // TODO hack. using a fragment as a way to restore context
//                String fragmentFile = sharedDir + "/SVP/fragmento_0.txt";
//                FileInputStream contextStream = new FileInputStream(fragmentFile);
//                frc.setExecutionContext(ExecutionContext.restoreFromStream(contextStream));
//                contextStream.close();
//                
//                File partialsDir = new File(sharedDir + "/partialResults");
//                File[] partialFiles = partialsDir.listFiles();
//                System.out.println("# of partial files: " + partialFiles.length);
//                List<String> partialFilenames = new ArrayList<String>();
//                for (File f : partialFiles) {
//                    partialFilenames.add(f.getAbsolutePath());
//                }
//                Collections.sort(partialFilenames);
//                
//                for(String partial : partialFilenames) {
//                    FileInputStream fis = new FileInputStream(partial);
//                    frc.loadPartial(fis);
//                    fis.close();
//                }
//
//                long t2 = System.nanoTime();
//                
//                System.out.println("Partials loading time: " + ((t2 - t1)/1000000) + " ms");
//                
//                frc.combinePartialResults();
//                
//                // Calcula o tempo de composição do resultado. Tempo retornado em milisegundos.
//                delay = ((System.nanoTime() - t2)/1000000);
//                System.out.println("Composition time: " + delay);
//                
//                long totalTime = ((System.nanoTime() - init)/1000000);
//                System.out.println("Total execution time: " + totalTime);
//                if ( out!=null ){
//                    out.close();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (DatabaseException e) {
//                e.printStackTrace();
//            }
        }       
        MPI.Finalize();
   }
    
    /**
     * Load the file content into a String object
     * 
     * @param filename The file
     * @return the content of the file in a string object
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static String readContentFromFile(String filename) throws FileNotFoundException, IOException {
        
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String everything = null;
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append('\n');
                line = br.readLine();
            }
            everything = sb.toString().trim();
        } finally {
            br.close();
        }
        
        return everything;
    }
    
    /**
     * Checks whether the query fulfills the requirements to process it. If
     * any problem is found, an exception is raised.<br/>
     *   1. must start and end with a constructor element<br/> 
     *   2. must not use the text() function (not supported)<br/>
     *   3. must have a XML element after return clause<br/>
     * 
     * @param query The query to be checked
     * @throws Exception
     */
    private static void verifyQuery(String query) throws Exception {
        String returnClause = query.substring(query.indexOf("return")+6, query.length());
        
        if (query.indexOf("<") > 0 || query.lastIndexOf(">") != query.length()-1) {
            throw new Exception("A consulta de entrada deve iniciar e "
                    + "terminar com um elemento construtor. Exemplo: <resultado> "
                    + "{ for $var in ... } </resultado>.");
        }
        else if (query.toUpperCase().indexOf("/TEXT()") != -1) {
            throw new Exception("O parser deste programa não aceita a função text(). "
                    + "Especifique somente os caminhos xpath para acessar os "
                    + "elementos nos documentos XML.");
        }
        else if (returnClause.trim().charAt(0) != '<') {
            
            throw new Exception("É obrigatória a especificação de um elemento XML após "
                    + "a cláusula return. Ex.: <results> { for $var ... return "
                    + "<elemName> ... </elemName> } </results>");
        }
    }
    
    
}
