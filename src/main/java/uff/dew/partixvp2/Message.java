package uff.dew.partixvp2;

public class Message {
    
    public static final int READY = 1;
    public static final int DONE = 2;
    public static final int FAIL = 3;
    public static final int WORK = 4;
    
    private int code;
    private int origin;
    private String payload;
    
    public Message(int code) {
        this(code,-1,null);
    }

    public Message(int code, int origin) {
        this(code, origin, null);
    }

    
    public Message(int code, int origin, String payload) {
        this.code = code;
        this.origin = origin;
        this.payload = payload;
    }
    
    public int getType() {
        return code;
    }
    
    public int getOrigin() {
        return origin;
    }
    
    public String getPayload() {
        return payload;
    }
    
    public Object[] getObjArray() {
        Object[] array = new Object[3];
        array[0] = new Integer(code);
        array[1] = new Integer(origin);
        array[2] = payload;
        return array;
    }
}
