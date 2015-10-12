package uff.dew.partixvp2;

import mpi.MPI;

public class MessageHelper {

    public static void sendReadyToMediator(int origin) {
        Message msg = new Message(Message.READY, origin);
        send(msg, 0);
    }
    
    public static void sendFailToMediator(int origin) {
        Message msg = new Message(Message.FAIL, origin);
        send(msg, 0);
    }

    public static void sendDoneToMediator(int origin, String payload) {
        Message msg = new Message(Message.DONE, origin, payload);
        send(msg, 0);
    }

    public static void sendWorkToPeer(int dest, String payload) {
        Message msg = new Message(Message.WORK, 0, payload);
        send(msg, dest);
    }
    
    public static void sendDoneToWorker(int dest) {
        Message msg = new Message(Message.DONE, 0);
        send(msg, dest);
    }
    
    public static Message recvFromPeer(int origin) {
        Object[] recvBuffer = new Object[3];
        MPI.COMM_WORLD.Recv(recvBuffer, 0, 3, MPI.OBJECT, origin, 0);
        int code = (Integer) recvBuffer[0];
        int msgOrigin = (Integer) recvBuffer[1];
        String payload = (String) recvBuffer[2];
        Message msg = new Message(code, msgOrigin, payload);
        return msg;
    }
    
    private static void send(Message msg, int dest) {
        Object[] array = msg.getObjArray();
        MPI.COMM_WORLD.Send(array, 0, array.length, MPI.OBJECT, dest, 0);
    }


}
