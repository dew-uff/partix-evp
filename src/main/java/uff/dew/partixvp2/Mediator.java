package uff.dew.partixvp2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mpi.MPI;

public class Mediator implements Runnable {

    private List<String> fragments;
    private List<String> partialFiles;
    
    public Mediator(List<String> fragments) {
        this.fragments = fragments;
        partialFiles = new ArrayList<String>();
    }
    
    public void run() {
        
        boolean working = true;
        int workersCount = 0;
        
        Iterator<String> fragIter = fragments.iterator();
        
        System.out.println("Mediator starting...");
        
        while (working) {
            
            Integer code, origin = null;
                    
            Object[] receiveBuffer = new Object[3];
            System.out.println("Mediator waiting to receive message from workers...");
            MPI.COMM_WORLD.Recv(receiveBuffer, 0, 3, MPI.OBJECT, MPI.ANY_SOURCE, 0);
            code = (Integer) receiveBuffer[0];
            origin = (Integer) receiveBuffer[1];
            System.out.println("Mediator received message from " + origin + " with code " + code);
            
            if (code.intValue() == Messages.READY.intValue()) {
                workersCount++;
            }
            else if (code.intValue() == Messages.DONE.intValue()) {
                String partialFile = (String) receiveBuffer[2];
                partialFiles.add(partialFile);
            }
                
            if (fragIter.hasNext()) {
                Object[] sendBuffer = new Object[3];
                sendBuffer[0] = Messages.WORK;
                sendBuffer[1] = fragIter.next();
                System.out.println("Mediator sending work to " + origin); 
                MPI.COMM_WORLD.Send(sendBuffer, 0, 3, MPI.OBJECT, origin, 0);
            }
            else {
                System.out.println("Mediator sending DONE message to worker " + origin);
                Object[] sendBuffer = new Object[3];
                sendBuffer[0] = Messages.DONE;
                MPI.COMM_WORLD.Send(sendBuffer, 0, 3, MPI.OBJECT, origin, 0);
                workersCount--;
                if (workersCount == 0) {
                    working = false;
                }
            }
        }
        
        System.out.println("Mediator finished sending fragments to process");
    }
}
