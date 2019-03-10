package cn.gx.storm.trident2.partitioned;//package com.wangwenjun.strom.example.trident2.partitioned;
//
//import java.io.Serializable;
//import java.util.Iterator;
//import java.util.List;
//
//public class PartitionList implements Iterable<Partition>, Serializable
//{
//    private final List<Partition> list;
//
//    public PartitionList(List<Partition> list)
//    {
//        this.list = list;
//    }
//
//    @Override
//    public Iterator<Partition> iterator()
//    {
//        return list.iterator();
//    }
//
//    public List<Partition> getList()
//    {
//        return list;
//    }
//
//    @Override
//    public String toString()
//    {
//        return "PartitionList{" +
//                "list=" + list +
//                '}';
//    }
//}
