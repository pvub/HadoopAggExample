package hadoopexample;

import java.util.StringTokenizer;

public class LineValue<T>
{
    public LineValue(Integer index, String type, String computation)
    {
        Index = index;
        Type = type;
        Computation = computation;
    }

    public LineValue(LineValue v)
    {
        Index = v.getIndex();
        Type = v.getType();
        Computation = v.getComputation();
        Value = (T) v.getValue();
    }
    
    public boolean MatchesIndex(Integer index)
    {
        return (Index == index);
    }

    public Integer getIndex()
    {
        return Index;
    }

    public String getType()
    {
        return Type;
    }

    public String getComputation()
    {
        return Computation;
    }
    
    public void setValue(T v)
    {
        Value = v;
    }
    
    public T getValue()
    {
        return Value;
    }

    public static String toString(LineValue v)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(v.getIndex()).append(":")
          .append(v.getType()).append(":")
          .append(v.getComputation()).append(":")
          .append(v.getValue().getClass().getSimpleName()).append(":")
          .append(v.getValue());
        return sb.toString();
    }
    
    private Integer Index;
    private String  Type;
    private String  Computation;
    private T       Value;
}
