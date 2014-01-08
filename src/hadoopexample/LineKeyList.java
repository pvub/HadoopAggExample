package hadoopexample;

import java.util.ArrayList;

public class LineKeyList
{
    public LineKeyList()
    {
        Keys = new ArrayList<LineKey>();
    }

    public void AddKey(LineKey key)
    {
        Keys.add(key);
    }

    public LineKey GetMatchingKey(Integer index)
    {
        for (LineKey k : Keys)
        {
            if (k.HasIndex(index))
            {
                return k;
            }
        }

        return null;
    }

    public ArrayList<LineKey> GetKeys()
    {
        return Keys;
    }

    private ArrayList<LineKey> Keys;
}
