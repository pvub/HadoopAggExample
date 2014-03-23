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
    
    private LineKeyList LineKeys;
    private HashMap<Integer, Column> Columns;
    
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
        Enumeration<Object> emKeys = prop.keys();
        while(emKeys.hasMoreElements()) 
        {
            String key = (String) emKeys.nextElement();
            if (key.startsWith(KEY_DEF)) 
            {
                String newKey = key.substring(KEY_DEF.length());
                String columnIndices = prop.getProperty(key);
                String[] strIndices = columnIndices.split("|");
                ArrayList<Integer> indices = new ArrayList<Integer>();
                for (String strIndex : strIndices)
                {
                    Integer columnindex = Integer.parseInt(strIndex);
                    indices.add(columnindex);
                }
                LineKey rowKey = new LineKey(new ArrayList<Integer>(indices));
                LineKeys.AddKey(rowKey);
            }
         }        
    }
    
    public LineKeyList getLineKeys()
    {
        return LineKeys;
    }
    
    private void loadColumns()
    {
        Enumeration<Object> emKeys = prop.keys();
        while(emKeys.hasMoreElements()) 
        {
            String key = (String) emKeys.nextElement();
            if (key.startsWith(COLUMN_DEF)) 
            {
                String strIndex = key.substring(COLUMN_DEF.length());
                String columnLine = prop.getProperty(key);
                String[] tokens = columnLine.split("|");
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
                Columns.put(iIndex, col);
            }
         }        
    }
    
    public HashMap<Integer, Column> getColumns()
    {
        return Columns;
    }
}
