/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoopexample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pvub
 */
public class MapperEnvelope implements WritableComparable<MapperEnvelope> {
    
    public MapperEnvelope()
    {
        entries = new ArrayList<LineValue>();
    }
    
    public void addValue(LineValue value)
    {
        entries.add(value);
    }
    
    public ArrayList<LineValue> getValues()
    {
        return entries;
    }
    
    public void setValues(ArrayList<LineValue> values)
    {
        entries.clear();
        entries.addAll(values);
    }
    
    private String serializeLineValue(LineValue v)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(v.getIndex()).append(":")
          .append(v.getType()).append(":")
          .append(v.getComputation()).append(":")
          .append(v.getValue().getClass().getSimpleName()).append(":")
          .append(v.getValue());
        return sb.toString();
    }
    
    private LineValue deserializeLineValue(String str)
    {
        LineValue v = null;
        
        StringTokenizer itr = new StringTokenizer(str,":");
        int iIndex = 0;
        
        Integer index = new Integer(0);
        String type = new String();
        String comp = new String();
        Integer intValue = new Integer(0);
        Double doubleValue = new Double(0.0);
        String strValue = new String();
        
        String typeName = new String();
        while (itr.hasMoreTokens()) 
        {
            String strField = itr.nextToken(":");
            switch (iIndex)
            {
                case 0:
                    index = Integer.parseInt(strField);
                    break;
                case 1:
                    type = strField;
                    break;
                case 2:
                    comp = strField;
                    break;
                case 3:
                    typeName = strField;
                    break;
                case 4:
                    if (Integer.class.getSimpleName().equalsIgnoreCase(typeName)) {
                        intValue = (Integer.parseInt(strField));
                        v = new LineValue<Integer>(index, type, comp);
                        v.setValue(intValue);
                    } else if (Double.class.getSimpleName().equalsIgnoreCase(typeName)) {
                        doubleValue = (Double.parseDouble(strField));
                        v = new LineValue<Double>(index, type, comp);
                        v.setValue(doubleValue);
                    } else {
                        strValue = strField;
                        v = new LineValue<String>(index, type, comp);
                        v.setValue(strValue);
                    }
            }
            ++iIndex;
        }
        
        return v;
    }
    
    private String serialize()
    {
        StringBuilder sb = new StringBuilder();
        for (LineValue v : entries)
        {
            String str = serializeLineValue(v);
            sb.append(str).append("|");
        }
        return sb.toString();
    }
    
    private void deserialize(String str)
    {
        entries.clear();
        System.out.println("deserialize: " + str);
        StringTokenizer itr = new StringTokenizer(str,"|");
        while (itr.hasMoreTokens()) 
        {
            String strLineValue = itr.nextToken("|");
            if (strLineValue != null && strLineValue.length() > 0)
            {
                LineValue v = deserializeLineValue(strLineValue);
                entries.add(v);
            }
        }
        System.out.println("Success: " + entries.size());
    }
    
    @Override
    public void write(DataOutput out) throws IOException 
    {
        String str = serialize();
        System.out.println("serialize: " + str);
        out.writeBytes(str);
    }

    @Override
    public void readFields(DataInput in) throws IOException 
    {
        deserialize(in.readLine());
    }

    @Override
    public int hashCode() {
      return entries.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof MapperEnvelope) {
        MapperEnvelope tp = (MapperEnvelope) o;
        return serialize().equalsIgnoreCase(tp.toString());
      }
      return false;
    }

    @Override
    public String toString() {
      return serialize();
    }

    @Override
    public int compareTo(MapperEnvelope tp) {
        return serialize().compareTo(tp.toString());
    }
  
    private ArrayList<LineValue> entries;
}
