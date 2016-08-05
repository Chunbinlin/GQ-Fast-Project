package gqfast.global;

import gqfast.global.Global.Encodings;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public class SocketServer {
	public static void getMessage(DataInputStream in) throws IOException {
		int stringLength;
		stringLength = in.readInt();

		System.out.println("received: " + stringLength);
		byte[] stringBytes = new byte[stringLength];
		for (int i=0; i<stringLength; i++) {
			stringBytes[i] = in.readByte();

		}
		String message = new String(stringBytes);
		System.out.println("received: " + message);

	}



	private static void sendMessage(DataOutputStream out, String message) throws IOException {
		
		String temp = message + "\n";
		System.out.println(temp);
		byte[] byteArray = temp.getBytes(StandardCharsets.UTF_8);
		
		System.out.println(temp.length());
		out.writeInt(temp.length());
		out.flush();
		out.write(byteArray);
		out.flush();
		
		
	}
	

	private static void sendMessage(IndexMetaMessage loadIndexDA1,
			DataOutputStream out) throws IOException {
		
		String messageInit = "Message init\0";
		byte[] bytes = messageInit.getBytes(StandardCharsets.UTF_8);
		out.write(bytes);
		out.flush();
		
		String loadIndex = "load_index\0";
		bytes = loadIndex.getBytes(StandardCharsets.UTF_8);
		out.write(bytes);
		out.flush();
		
		
		String path = loadIndexDA1.getPathAndFilename() + "\0";
		bytes = path.getBytes(StandardCharsets.UTF_8);
		out.write(bytes);
		out.flush();
		int numCols = loadIndexDA1.getNumEncodedCols();
		out.writeBytes(numCols + "");
		out.flush();
		String[] names = loadIndexDA1.getEncodedColNames();
		Encodings[] encs = loadIndexDA1.getEncodedColEncodings();
		for (int i=0; i<numCols; i++) {
			String name = names[i] + "\0";
			bytes = name.getBytes(StandardCharsets.UTF_8);
			out.write(bytes);
			out.flush();
			switch (encs[i]) {
			case Uncompress_Array:
				out.writeInt(1);
				break;
			case Bit_Compress_Array:
				out.writeInt(2);
				break;
			case Byte_Align_Bitmap:
				out.writeInt(3);
				break;
			case Huffman:
				out.writeInt(4);
				break;
			}
			out.flush();
		}
		
		String messageEnd = "Message terminate\0";
		bytes = messageEnd.getBytes(StandardCharsets.UTF_8);
		out.write(bytes);
		out.flush();
	}

	public static void main(String[] args) throws IOException {


        String hostName = "localhost";
        int portNumber = 7235;
        
        
        
        
        Socket firstSocket = new Socket(hostName, portNumber);
      
        
        DataInputStream in = new DataInputStream(firstSocket.getInputStream());
        DataOutputStream out = new DataOutputStream(firstSocket.getOutputStream());
        
        getMessage(in);
        
        String path = "./pubmed/da1.csv";
        int num_encodings = 1;
        String[] names = {"Doc"};
        Encodings[] encodings = {Encodings.Uncompress_Array};
        IndexMetaMessage loadIndexDA1 = new IndexMetaMessage(path, num_encodings, names, encodings);
        
    	sendMessage(loadIndexDA1, out);
        
    	String shutdown = "Shutdown signal\0";
    	byte[] bytes = shutdown.getBytes(StandardCharsets.UTF_8);
    	out.write(bytes);
    	out.flush();
        
        		
        in.close();
        out.close();
        firstSocket.close();

    }






}
