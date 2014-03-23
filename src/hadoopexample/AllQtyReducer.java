/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoopexample;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pvub
 */
public class AllQtyReducer extends Reducer<Text,MapperEnvelope,Text,MapperEnvelope>
{
    private ApplicationProperties Prop = null;
    private MapperEnvelope result = new MapperEnvelope();
    private LineValueList valueList = null;
    private int iCounter = 0;
    
    @Override
    protected void setup(Reducer.Context context) throws IOException,
                InterruptedException
    {
        if (Prop != null)
            return;
        
        try 
        {
            System.out.println("#@# Reducer Loading Properties file");
            Configuration conf = context.getConfiguration();
            String propFile = conf.get("AppProperties");
            Prop = new ApplicationProperties();
            Prop.load(propFile);
            valueList = Prop.getLineValues();
        } catch (Exception e) {
            System.out.println("#@# Mapper: " + e.getMessage());
            //handle the exception
        }
    }
    
    public void reduce(Text key, Iterable<MapperEnvelope> values, Reducer.Context context) 
            throws IOException, InterruptedException
    {
//            Integer totalQty = new Integer(0);
//            for (IntWritable val : values)
//            {
//                totalQty += val.get();
//            }
        System.out.println("#@# reducer key:" + key + " Count: " + ++iCounter);

        int iCount = 0;

        for (MapperEnvelope e : values)
        {
            //System.out.println("#@# reducer Envelope: " + e.toString());
            ++iCount;
            ArrayList<LineValue> eValues = e.getValues();
            for (LineValue eValue : eValues)
            {
                //System.out.println("#@# LineValue: " + LineValue.toString(eValue));
                LineValue trackingValue = valueList.getMatchingLineValue(eValue.getIndex());
                //System.out.println("#@# trackingValue: " + LineValue.toString(trackingValue));
                if (trackingValue.getType().equalsIgnoreCase("int"))
                {
                    Integer intValue = (Integer) eValue.getValue();
                    Integer whatIHave = (Integer) trackingValue.getValue();
                    if (eValue.getComputation().equalsIgnoreCase("weightedmean"))
                    {
                        trackingValue.setValue(whatIHave + (1 * intValue));
                    }
                    else
                    {
                        trackingValue.setValue(whatIHave + intValue);
                    }
                }
                else if (trackingValue.getType().equalsIgnoreCase("double"))
                {
                    Double doubleValue = (Double) eValue.getValue();
                    Double whatIHave = (Double) trackingValue.getValue();
                    if (eValue.getComputation().equalsIgnoreCase("weightedmean"))
                    {
                        trackingValue.setValue(whatIHave + (1 * doubleValue));
                    }
                    else
                    {
                        trackingValue.setValue(whatIHave + doubleValue);
                    }
                }
            }
        }

        ArrayList<LineValue> sumValues = valueList.getValuesAsArray();
        System.out.println("#@# reducer key:" + key + " valueList:" + sumValues.size() + " iCount: " + iCount);
        for (LineValue v : sumValues)
        {
            if (v.getComputation().equalsIgnoreCase("mean"))
            {
                if (v.getType().equalsIgnoreCase("int"))
                {
                    v.setValue(((Integer) v.getValue())/iCount);
                }
                else if (v.getType().equalsIgnoreCase("double"))
                {
                    v.setValue(((Double) v.getValue())/iCount);
                }
            }
            else if (v.getComputation().equalsIgnoreCase("weightedmean"))
            {
                if (v.getType().equalsIgnoreCase("int"))
                {
                    v.setValue(((Integer) v.getValue())/iCount);
                }
                else if (v.getType().equalsIgnoreCase("double"))
                {
                    v.setValue(((Double) v.getValue())/iCount);
                }
            }
        }

        result.setValues(valueList.getValuesAsArray());
        System.out.println("#@# reducer key:" + key + " result:" + result);
        context.write(key, result);
        valueList = Prop.getLineValues();
    }
}
