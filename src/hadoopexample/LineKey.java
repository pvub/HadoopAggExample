package hadoopexample;

import java.util.ArrayList;

public class LineKey
{
    public LineKey(ArrayList<Integer> columnIndices)
    {
        ColumnIndices = new ArrayList<Integer>(columnIndices);
        KeyFields = new ArrayList<String>();
    }

    public boolean HasIndex(Integer index)
    {
        return ColumnIndices.contains(index);
    }

    public void AddKeyField(String fieldValue)
    {
        KeyFields.add(fieldValue);
    }

    public String ToString()
    {
        StringBuilder sb = new StringBuilder();
        for (String f : KeyFields)
        {
            sb.append(f).append("|");
        }
        return sb.toString();
    }

    private ArrayList<Integer> ColumnIndices;
    // Store the Key Strings
    private ArrayList<String>  KeyFields;
}
