package hadoopexample;

public class LineValue<T>
{
    public LineValue(Integer index, String type, String computation)
    {
        Index = index;
        Type = type;
        Computation = computation;
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

    private Integer Index;
    private String  Type;
    private String  Computation;
    private T       Value;
}
