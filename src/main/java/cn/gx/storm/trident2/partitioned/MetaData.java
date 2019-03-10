package cn.gx.storm.trident2.partitioned;

import java.io.Serializable;

public class MetaData implements Serializable
{

    private final int start;
    private final int end;

    public MetaData(int start, int end)
    {
        this.start = start;
        this.end = end;
    }

    public int getStart()
    {
        return start;
    }

    public int getEnd()
    {
        return end;
    }

    @Override
    public String toString()
    {
        return "MetaData{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}