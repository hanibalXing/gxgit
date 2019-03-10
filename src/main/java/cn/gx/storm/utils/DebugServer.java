package cn.gx.storm.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class DebugServer
{
    private final static int PORT = 12011;

    public static void main(String[] args)
    {
        try
        {
            ServerSocket server = new ServerSocket(PORT);
            while (true)
            {
                final Socket client = server.accept();
                new Thread(() ->
                {
                    try
                    {
                        BufferedReader buffer = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        String line;
                        while ((line = buffer.readLine()) != null)
                        {
                            System.out.println(line);
                        }
                    } catch (IOException e)
                    {
                        //ignore the exception
                    }
                }).start();
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
