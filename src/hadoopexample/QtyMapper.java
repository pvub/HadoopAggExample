/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoopexample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pvub
 */
public class QtyMapper extends Mapper<Text, Text, Text, MapperEnvelope>
{
    private ApplicationProperties Prop = null;
    
    @Override
    protected void setup(Context context) throws IOException,
                InterruptedException
    {
        if (Prop != null)
            return;
        try 
        {
            System.out.println("#@# Mapper Loading Properties file");
            Configuration conf = context.getConfiguration();
            String propFile = conf.get("AppProperties");
            Prop = new ApplicationProperties();
            Prop.load(propFile);
            System.out.println("#@# Log Line Values: " + Prop.getLineValues().toString());
        } catch (Exception e) {
            System.out.println("#@# Mapper: " + e.getMessage());
            //handle the exception
        }
    }

    private Integer getInt(String strValue)
    {
        Integer i = new Integer(0);
        try
        {
            i = Integer.parseInt(strValue);
            //System.out.println("#@# mapper rowQty:" + i);
        }
        catch (NumberFormatException e) {
            ;
        }
        return i;
    }

    private Double getDouble(String strValue)
    {
        Double d = new Double(0.0);
        try
        {
            d = Double.parseDouble(strValue);
            //System.out.println("#@# mapper rowQty:" + d);
        }
        catch (NumberFormatException e) {
            ;
        }
        return d;
    }

    public void map(Text key, Text value, Mapper.Context context) throws IOException, InterruptedException
    {
        LineKeyList rowKeyList = Prop.getLineKeys();
        LineValueList rowValueList = Prop.getLineValues();
        
        System.out.println("#@# Mapper Line Values: " + rowValueList.toString());

        //IntWritable qty = new IntWritable();
        MapperEnvelope envelope = new MapperEnvelope();

        StringTokenizer itr = new StringTokenizer(key.toString(),"|");
        int iIndex = 0;
        LineKey k = null;
        LineValue v = null;
        int rowQty = 0;
        System.out.println("#@# mapper tokens:" + itr.countTokens() + " line:" + key);
        while (itr.hasMoreTokens())
        {
            k = rowKeyList.GetMatchingKey(iIndex);
            v = rowValueList.getMatchingLineValue(iIndex);

            if (k != null) 
            {
                String strFieldValue = itr.nextToken("|");
                k.AddKeyField(strFieldValue);
            }
            else if (v != null)
            {
                String strQty = itr.nextToken("|");
                System.out.println("#@# mapper strQty:" + strQty);

                if (v.getType().equalsIgnoreCase("int"))
                {
                    Integer intValue = getInt(strQty);
                    LineValue lv = new LineValue<Integer>(v);
                    lv.setValue(intValue);
                    envelope.addValue(lv);
                }
                else if (v.getType().equalsIgnoreCase("double"))
                {
                    Double doubleValue = getDouble(strQty);
                    LineValue lv = new LineValue<Double>(v);
                    lv.setValue(doubleValue);
                    envelope.addValue(lv);
                }

//                    try
//                    {
//                        rowQty = Integer.parseInt(strQty);
//                        System.out.println("#@# mapper rowQty:" + rowQty);
//                    }
//                    catch (NumberFormatException e) {
//                        ;
//                    }

            }
            else
            {
                itr.nextToken("|");
            }
            ++iIndex;
        }

        //qty = new IntWritable(rowQty);

        ArrayList<LineKey> keys = rowKeyList.GetKeys();
        for (LineKey lk : keys)
        {
            key = new Text(lk.ToString());
            System.out.println("#@# mapper out key:" + key + " envelope:" + envelope.toString());
            context.write(key, envelope);
        }
    }
}
