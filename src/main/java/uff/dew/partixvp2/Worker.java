package uff.dew.partixvp2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import uff.dew.svp.SubQueryExecutionException;
import uff.dew.svp.SubQueryExecutor;
import uff.dew.svp.db.DatabaseException;
import uff.dew.svp.fragmentacaoVirtualSimples.SubQuery;

public class Worker implements Participant {

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
        this.sharedDir = sharedDir;
    }
    
    public boolean run() {
        
        boolean errors = false;
        
        System.out.println("Worker " + id + " starting");
        
        // it will block waiting mediator to receive this message
        MessageHelper.sendReadyToMediator(id);
        
        int code = Message.READY;
        
        // it will iterate until mediator sends a DONE message to it        
        while (code != Message.DONE && code != Message.FAIL) {
            
            // block until receive a message from mediator
            Message msg = MessageHelper.recvFromPeer(0);
            System.out.println("Worker " + id + " received message type " + msg.getType());
            
            code = msg.getType();
            
            if (code == Message.WORK) {

                String fragment = msg.getPayload();
                
                String resultFile;
                try {
                    resultFile = processFragment(fragment);
                    System.out.println("Worker "+ id + " sending DONE to Mediator... ");
                    MessageHelper.sendDoneToMediator(id, resultFile);
                } catch (Exception e) {
                    // TODO remove print stack trace
                    e.printStackTrace();
                    System.out.println("Worker "+ id + " sending FAIL to Mediator... ");
                    MessageHelper.sendFailToMediator(id);
                    errors = true;
                    break;
                }
            }
            else if (code == Message.FAIL) {
                errors = true;
            }
        }
        
        System.out.println("Worker " + id + " has finished its work");
        
        return !errors;
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
