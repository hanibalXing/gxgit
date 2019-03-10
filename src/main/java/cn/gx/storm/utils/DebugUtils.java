package cn.gx.storm.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.util.Date;

import static java.lang.Thread.currentThread;

public class DebugUtils
{
    public static void debug(String message, Object obj)
    {
        try (Socket socket = new Socket("192.168.88.6", 12011);
             PrintWriter writer = new PrintWriter(socket.getOutputStream()))
        {
            writer.println(new Date() + ":" + getProcess() + "-" + currentThread() + "-" + obj.hashCode() + "-" + message);
            writer.flush();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static String getProcess()
    {
        return ManagementFactory.getRuntimeMXBean().getName();
    }
}
