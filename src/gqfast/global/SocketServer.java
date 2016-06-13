package gqfast.global;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SocketServer {
    public static void main(String[] args) throws IOException {


        String hostName = "localhost";
        int portNumber = 7235;
        
        String message = "Message init\n";
        message += "Load index\n";
        message += "Message terminate\n";
        message += "Shutdown signal\n";
        
        InputStream stream = new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8));
        
        Socket firstSocket = new Socket(hostName, portNumber);
        PrintWriter out = new PrintWriter(firstSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(firstSocket.getInputStream()));
        BufferedReader messageIn = new BufferedReader(new InputStreamReader(stream));
        System.out.println("received: " + in.readLine());
        String userInput;
        while ((userInput = messageIn.readLine()) != null) 
        {
            out.println(userInput);
            System.out.println("received: " + in.readLine());
        }
        System.out.println("received: " + in.readLine());
        
        in.close();
        messageIn.close();
        firstSocket.close();

    }
}
