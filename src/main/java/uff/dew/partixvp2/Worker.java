package uff.dew.partixvp2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import uff.dew.svp.SubQueryExecutionException;
import uff.dew.svp.SubQueryExecutor;
import uff.dew.svp.db.DatabaseException;
import uff.dew.svp.fragmentacaoVirtualSimples.SubQuery;
import mpi.MPI;

public class Worker implements Runnable {

    private int id;
    private String sharedDir;
    private String dbhost;
    private int dbport;
    private String dbusername;
    private String dbpassword;
    private String dbname;
    private String dbtype;
    
    public Worker(int id, String sharedDir, String dbhost, int dbport, String dbusername, 
            String dbpassword, String dbname, String dbtype) {
        this.id = id;
        this.dbhost = dbhost;
        this.dbport = dbport;
        this.dbusername = dbusername;
        this.dbpassword = dbpassword;
        this.dbname = dbname;
        this.dbtype = dbtype;
    }
    
    public void run() {
        System.out.println("Worker " + id + " starting...");
        
        Object[] sendBuffer = new Object[3];
        sendBuffer[0] = Messages.READY;
        sendBuffer[1] = new Integer(id);
        MPI.COMM_WORLD.Send(sendBuffer, 0, 3, MPI.OBJECT, 0, 0);
        
        Integer code = Messages.READY;
        
        while (code.intValue() != Messages.DONE.intValue()) {
            Object[] recvBuffer = new Object[3];
            MPI.COMM_WORLD.Recv(recvBuffer, 0, 3, MPI.OBJECT, 0, 0);
            code = (Integer) recvBuffer[0];
            System.out.println("Worker " + id + " received message with code " + code);
            
            if (code.intValue() == Messages.WORK.intValue()) {
                String fragment = (String) recvBuffer[1];
                System.out.println("Worker "+ id + " would process a fragment");
                
                String resultFile;
                try {
                    resultFile = processFragment(fragment);
                } catch (Exception e) {
                    e.printStackTrace();
                    sendBuffer[0] = Messages.FAIL;
                    sendBuffer[1] = id;
                    System.out.println("Worker "+ id + " sending FAIL to Mediator... ");
                    MPI.COMM_WORLD.Send(sendBuffer, 0, 3, MPI.OBJECT, 0, 0);
                    break;
                } 
                
                sendBuffer[0] = Messages.DONE;
                sendBuffer[1] = id;
                sendBuffer[2] = resultFile;
                System.out.println("Worker "+ id + " sending DONE to Mediator... ");
                MPI.COMM_WORLD.Send(sendBuffer, 0, 3, MPI.OBJECT, 0, 0);
            }
            else if (code.intValue() == Messages.DONE.intValue()) {
                System.out.println("Worker " + id + "received DONE");
            }
        }
    }

    private String processFragment(String fragment) throws DatabaseException, SubQueryExecutionException, IOException {
        
        SubQueryExecutor sqe = new SubQueryExecutor(fragment);
        sqe.setDatabaseInfo(dbhost, dbport, dbusername, dbpassword, dbname, dbtype);
        
        String query = sqe.getExecutionContext().getQueryObj().getInputQuery();
        SubQuery sbq = sqe.getExecutionContext().getSubQueryObj();
        String filePath = "partialResult_intervalBeginning_"+ String.format("%1$020d", Integer.parseInt(SubQuery.getIntervalBeginning(query,sbq))) + ".xml";
        String completePath = sharedDir + "/partialResults/" + filePath;
        FileOutputStream fos = new FileOutputStream(completePath);
        
        boolean hasResults = sqe.executeQuery(fos);
        fos.flush();
        fos.close();
        
        if (!hasResults) {
            File resultFile = new File(completePath);
            resultFile.delete();
            return null;
        }
        
        return completePath;
    }
}
