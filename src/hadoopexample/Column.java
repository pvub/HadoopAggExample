/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoopexample;

/**
 *
 * @author pvub
 */
public class Column {
    
    public Column(String name, Integer index, String type, String group)
    {
        this.Name = name;
        this.DataType = getType(type);
        this.Index = index;
        this.Group = group;
    }

    /**
     * @return the Name
     */
    public String getName() {
        return Name;
    }

    /**
     * @param Name the Name to set
     */
    public void setName(String Name) {
        this.Name = Name;
    }

    /**
     * @return the Index
     */
    public Integer getIndex() {
        return Index;
    }

    /**
     * @param Index the Index to set
     */
    public void setIndex(Integer Index) {
        this.Index = Index;
    }

    /**
     * @return the DataType
     */
    public Column.Type getDataType() {
        return DataType;
    }

    /**
     * @param DataType the DataType to set
     */
    public void setDataType(Column.Type DataType) {
        this.DataType = DataType;
    }

    /**
     * @return the Group
     */
    public String getGroup() {
        return Group;
    }

    /**
     * @param Group the Group to set
     */
    public void setGroup(String Group) {
        this.Group = Group;
    }
    
    public static Column.Type getType(String type)
    {
        if (type.equalsIgnoreCase("string"))
            return Column.Type.STRING;
        if (type.equalsIgnoreCase("int"))
            return Column.Type.INTEGER;
        if (type.equalsIgnoreCase("double"))
            return Column.Type.DOUBLE;
        return Column.Type.NONE;
    }
    
    public enum Type
    {
        NONE,
        STRING,
        INTEGER,
        DOUBLE
    }
    
    private String          Name;
    private Integer         Index;
    private Column.Type     DataType;
    private String          Group;
    
}
