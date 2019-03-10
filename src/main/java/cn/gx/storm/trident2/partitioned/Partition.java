package cn.gx.storm.trident2.partitioned;

import org.apache.storm.trident.spout.ISpoutPartition;

import java.io.Serializable;

public class Partition implements ISpoutPartition, Serializable
{

    private final String id;

    public Partition(String id)
    {
        this.id = id;
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Partition{" +
                "id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Partition partition = (Partition) o;

        return id != null ? id.equals(partition.id) : partition.id == null;
    }

    @Override
    public int hashCode()
    {
        return id != null ? id.hashCode() : 0;
    }
}
