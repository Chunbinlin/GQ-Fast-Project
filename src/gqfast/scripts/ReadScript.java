package gqfast.scripts;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

import java.io.InputStream;

public class ReadScript {

	
	 private static String loadStream(InputStream s) throws Exception
	    {
	        BufferedReader br = new BufferedReader(new InputStreamReader(s));
	        StringBuilder sb = new StringBuilder();
	        String line;
	        while((line=br.readLine()) != null)
	            sb.append(line).append("\n");
	        return sb.toString();
	    }

	public static void readBashScript() {
		try {
			Process proc = Runtime.getRuntime().exec("/home/ben/git/GQ-Fast-Final/myscript.sh"); //Whatever you want to execute
			BufferedReader read = new BufferedReader(new InputStreamReader(
					proc.getInputStream()));
			try {
				proc.waitFor();
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
			while (read.ready()) {
				System.out.println(read.readLine());
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}



	public static void main(String args[]) throws IOException
	{
		/*ProcessBuilder pb = new ProcessBuilder("/home/ben/git/GQ-Fast-Final/myscript.sh");
		 //Map<String, String> env = pb.environment();
		 //env.put("VAR1", "myValue");
		 //env.remove("OTHERVAR");
		 //env.put("VAR2", env.get("VAR1") + "suffix");
		 pb.redirectInput();
		 pb.directory(new File("/home/ben/git/GQ-Fast-Final/"));
		 pb.start();
		 */

		String dataFile = "dt1_mesh.csv";
		String configFile = "config_dt1_bb.xml";

		String[] command = {"/bin/bash", "myscript.sh", "dt1_mesh.csv", "config_dt1_bb.xml"};
		ProcessBuilder p = new ProcessBuilder(command);
		
		File f = new File("/home/ben/git/GQ-Fast-Final/");
		p.directory(f);
		Process process = p.start();


		String output = "";
		String error = "";
		try {
			output = loadStream(process.getInputStream());
			error  = loadStream(process.getErrorStream());
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        int rc = -1;
		try {
			rc = process.waitFor();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        System.out.println("Process ended with rc=" + rc);
        System.out.println("\nStandard Output:\n");
        System.out.println(output);
        System.out.println("\nStandard Error:\n");
        System.out.println(error);

		//Wait to get exit value
		try {
			int exitValue = process.waitFor();
			System.out.println("\n\nExit Value is " + exitValue);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//readBashScript();


	}
	
}
