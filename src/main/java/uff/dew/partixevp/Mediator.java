package uff.dew.partixevp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mpi.MPI;

public class Mediator implements Participant {

    private List<String> fragments;
    private List<String> partialFiles;
    private List<Integer> workers;
    
    public Mediator(List<String> fragments) {
        this.fragments = fragments;
        partialFiles = new ArrayList<String>();
        workers = new ArrayList<Integer>();
    }
    
    public boolean run() {
        
        boolean working = true;
        boolean errors = false;
        
        Iterator<String> fragIter = fragments.iterator();
        
        System.out.println("Mediator starting...");
        
        while (working) {
            
            System.out.println("Mediator waiting to receive message from workers...");
            Message msg = MessageHelper.recvFromPeer(MPI.ANY_SOURCE);
            long timestamp = System.currentTimeMillis();
            System.out.println("Mediator received message from " + msg.getOrigin() + 
                    " with code " + msg.getType());
            System.out.println("Mediator 0 START " + timestamp);
            
            switch (msg.getType()) {
            
                case Message.READY:
                    // each worker is supposed to send a READY. so we can rely on this to count
                    // how many workers we have
                    workers.add(msg.getOrigin());
                    break;
                
                case Message.DONE:
                    // when message is DONE, the payload contains reference to the partial
                    partialFiles.add(msg.getPayload());
                    long workerTimestamp = System.currentTimeMillis();
                    System.out.println("Worker " + msg.getOrigin() + " END " + workerTimestamp);
                    break;
                    
                case Message.FAIL:
                    // when FAIL is received, send DONE to every other
                    System.out.println("Something went wrong!");
                    errors = true;
                    break;
            }

            // there is still fragment to send
            if (!errors && fragIter.hasNext()) {
                System.out.println("Mediator sending work to " + msg.getOrigin()); 
                MessageHelper.sendWorkToPeer(msg.getOrigin(), fragIter.next());
                // register timestamp of execution for this worker
                long workerTimestamp = System.currentTimeMillis();
                System.out.println("Worker " + msg.getOrigin() + " START " + workerTimestamp);
            }
            else {
                System.out.println("Mediator sending DONE message to worker " + msg.getOrigin());
                MessageHelper.sendDoneToWorker(msg.getOrigin());
                workers.remove(new Integer(msg.getOrigin()));
                if (workers.isEmpty()) {
                    working = false;
                }
            }
            
            timestamp = System.currentTimeMillis();
            System.out.println("Mediator 0 END " + timestamp);
        }
        System.out.println("Mediator finished sending fragments to process");
        
        return !errors;
    }
}
