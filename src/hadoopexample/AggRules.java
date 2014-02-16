/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoopexample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* cd ~/Development/hadoop/hadoop-1.2.1 */
/* hadoop jar HadoopAggRulesExample.jar Dictionary Italian.txt output.txt */
/* cd output */
public class AggRules
{            
    public static class QtyMapper extends Mapper<Text, Text, Text, MapperEnvelope>
    {                        
        private LineKeyList createKeys()
        {
            LineKeyList keys = new LineKeyList();

            ArrayList<Integer> indices = new ArrayList<Integer>();
            indices.add(6); // TOTAL
            indices.add(9); // NATIONAL
            LineKey rowKey1 = new LineKey(new ArrayList<Integer>(indices));
            keys.AddKey(rowKey1);

            indices.clear();
            indices.add(5); // PLID
            indices.add(8); // RETAILER
            LineKey rowKey2 = new LineKey(new ArrayList<Integer>(indices));
            keys.AddKey(rowKey2);

            return keys;
        }

        private LineValueList createValues()
        {
            LineValueList values = new LineValueList();

            LineValue totalstores = new LineValue<Integer>(10, "int", "sum");
            values.AddLineValue(totalstores);

            LineValue sin1 = new LineValue<Integer>(21, "double", "mean");
            values.AddLineValue(sin1);
            
            LineValue cos1 = new LineValue<Integer>(22, "double", "mean");
            values.AddLineValue(cos1);
            
            LineValue avgbp = new LineValue<Integer>(27, "double", "weightedmean");
            values.AddLineValue(avgbp);
            
            return values;
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

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            LineKeyList rowKeyList = createKeys();
            LineValueList rowValueList = createValues();
            
            //IntWritable qty = new IntWritable();
            MapperEnvelope envelope = new MapperEnvelope();
            
            StringTokenizer itr = new StringTokenizer(key.toString(),"|");
            int iIndex = 0;
            LineKey k = null;
            LineValue v = null;
            int rowQty = 0;
            //System.out.println("#@# mapper tokens:" + itr.countTokens() + " line:" + key);
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
                    //System.out.println("#@# mapper strQty:" + strQty);
                    
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
                //System.out.println("#@# mapper out key:" + key + " envelope:" + envelope.toString());
                context.write(key, envelope);
            }
        }
    }
    
    public static class AllQtyReducer
    extends Reducer<Text,MapperEnvelope,Text,MapperEnvelope>
    {
        private LineValueList createValues()
        {
            LineValueList values = new LineValueList();

            LineValue totalstores = new LineValue<Integer>(10, "int", "sum");
            totalstores.setValue(0);
            values.AddLineValue(totalstores);

            LineValue sin1 = new LineValue<Integer>(21, "double", "mean");
            sin1.setValue(0.0);
            values.AddLineValue(sin1);
            
            LineValue cos1 = new LineValue<Integer>(22, "double", "mean");
            cos1.setValue(0.0);
            values.AddLineValue(cos1);
            
            LineValue avgbp = new LineValue<Integer>(27, "double", "weightedmean");
            avgbp.setValue(0.0);
            values.AddLineValue(avgbp);
            
            return values;
        }
        
        private MapperEnvelope result = new MapperEnvelope();
        private LineValueList valueList = createValues();
        private int iCounter = 0;
        public void reduce(Text key, Iterable<MapperEnvelope> values, Context context) 
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
            valueList = createValues();
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "aggrules");
        job.setJarByClass(AggRules.class);
        job.setMapperClass(QtyMapper.class);
        job.setReducerClass(AllQtyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapperEnvelope.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}