/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoopexample;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

/**
 *
 * @author pvub
 */
public class ApplicationProperties {
    
    private Properties prop;
    
    private static String KEY_DEF="key.";
    private static String VALUE_DEF="value.";
    private static String COLUMNS_DEF="input.columns";
    private static String COLUMN_DEF="input.column.";
    
    private LineKeyList LineKeys = null;
    private HashMap<Integer, Column> Columns = null;
    private LineValueList LineValues = null;
    
    public ApplicationProperties()
    {
    }
    
    public void load(String filename)
    {
        prop = new Properties();
        try
        {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream(filename);
            prop.load(in);
        }
        catch(Exception e)
        {
            System.out.println("#@# Error loading file: " + e.getMessage());
        }
        
        loadColumns();
        loadKeys();
        loadValues();
    }
    
    public String getProperty(String key)
    {
        if (prop != null)
        {
            return prop.getProperty(key);
        }
        return null;
    }
    
    public void print()
    {
        Set entries = prop.keySet();
        Iterator<String> iterator = entries.iterator();
        while(iterator.hasNext()) 
        {
            String key = (String) iterator.next();
            System.out.println(prop.getProperty(key));
        }
    
    }
    
    private void loadKeys()
    {
        if (LineKeys == null) LineKeys = new LineKeyList();
        Enumeration<Object> emKeys = prop.keys();
        while(emKeys.hasMoreElements()) 
        {
            String key = (String) emKeys.nextElement();
            if (key.startsWith(KEY_DEF)) 
            {
                String newKey = key.substring(KEY_DEF.length());
                String columnIndices = prop.getProperty(key);
                String[] strIndices = columnIndices.split("\\|");
                ArrayList<Integer> indices = new ArrayList<Integer>();
                for (String strIndex : strIndices)
                {
                    System.out.println("#@# key: " + strIndex);
                    Integer columnindex = Integer.parseInt(strIndex);
                    indices.add(columnindex);
                }
                LineKey rowKey = new LineKey(new ArrayList<Integer>(indices));
                LineKeys.AddKey(rowKey);
            }
         }        
        System.out.println("#@# Loaded Keys Size: " + LineKeys.GetKeys().size());
    }
    
    public LineKeyList getLineKeys()
    {
        return LineKeys;
    }
    
    private void loadColumns()
    {
        if (Columns == null) Columns = new HashMap<Integer, Column>();
        Enumeration<Object> emKeys = prop.keys();
        while(emKeys.hasMoreElements()) 
        {
            String key = (String) emKeys.nextElement();
            if (key.startsWith(COLUMN_DEF)) 
            {
                String strIndex = key.substring(COLUMN_DEF.length());
                String columnLine = prop.getProperty(key);
                String[] tokens = columnLine.split("\\|");
                int i = 0;
                Integer iIndex = Integer.parseInt(strIndex);
                String strName = "", strType = "", strGroup = "";
                for (String token : tokens)
                {
                    if (i == 0)
                    {
                        strName = token;
                    }
                    else if (i == 1)
                    {
                        strType = token;
                    }
                    else if (i == 2)
                    {
                        strGroup = token;
                    }
                    ++i;
                }
                Column col = new Column(strName, iIndex, strType, strGroup);
                System.out.println("#@# Adding col ind:" + iIndex + " name:" + col.getName());
                Columns.put(iIndex, col);
            }
         }    
        System.out.println("#@# Loaded Columns Size: " + Columns.size());
    }
    
    public Column getColumn(Integer index)
    {
        return Columns.get(index);
    }
    
    public HashMap<Integer, Column> getColumns()
    {
        return Columns;
    }
    
    private void loadValues()
    {
        if (LineValues == null) LineValues = new LineValueList();
        Enumeration<Object> emvalues = prop.keys();
        while(emvalues.hasMoreElements())
        {
            String key = (String) emvalues.nextElement();
            if (key.startsWith(VALUE_DEF))
            {
                String newVal = key.substring(VALUE_DEF.length());
                
                String columnValues = prop.getProperty(key);
                System.out.println("#@# Value Line: " + columnValues);
                
                String[] tokens = columnValues.split("\\|");
                Integer columnindex = Integer.parseInt(tokens[0]);
                String agg         = tokens[1];
                Column col          = getColumn(columnindex);
                System.out.println("#@# inx:" + columnindex + " agg:" + agg);
                
                if (col.getDataType() == Column.Type.INTEGER)
                {
                    System.out.println("#@# Int: " + col.getName());
                    LineValue lineValue = new LineValue<Integer>(columnindex, "int", agg);
                    LineValues.AddLineValue(lineValue);
                }
                else if (col.getDataType() == Column.Type.DOUBLE)
                {
                    System.out.println("#@# double: " + col.getName());
                    LineValue lineValue = new LineValue<Integer>(columnindex, "double", agg);
                    LineValues.AddLineValue(lineValue);
                }
            }
        }
        System.out.println("#@# Loaded Values Size: " + LineValues.getValueList().size());
    }

    public LineValueList getLineValues()
    {
        return LineValues;
    }
    
    public Properties getProperties()
    {
        return prop;
    }
}
