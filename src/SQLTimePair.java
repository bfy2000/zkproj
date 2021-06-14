import java.io.Serializable;

public class SQLTimePair implements Serializable {
    public Long timeStamp;
    public String clause;

    public SQLTimePair(Long a,String b){
        timeStamp = a;
        clause = b;
    }
}
