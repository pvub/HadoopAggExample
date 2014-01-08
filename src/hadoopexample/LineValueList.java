package hadoopexample;

import java.util.HashMap;

public class LineValueList
{
    public LineValueList()
    {
        Values = new HashMap<Integer, LineValue>();
    }

    public void AddLineValue(LineValue value)
    {
        Values.put(value.getIndex(), value);
    }

    public HashMap<Integer, LineValue> getValueList()
    {
        return Values;
    }

    public LineValue getMatchingLineValue(Integer index)
    {
        return (LineValue) Values.get((Integer)index);
    }

    private HashMap<Integer, LineValue> Values;
}
