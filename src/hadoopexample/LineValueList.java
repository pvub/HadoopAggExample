package hadoopexample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

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
    
    public ArrayList<LineValue> getValuesAsArray()
    {
        ArrayList<LineValue> values = new ArrayList<LineValue>();
        for (Entry<Integer, LineValue> e : Values.entrySet())
        {
            values.add(e.getValue());
        }
        return values;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (Entry<Integer, LineValue> e : Values.entrySet())
        {
            sb.append(e.getValue().toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    private HashMap<Integer, LineValue> Values;
}
