package com.abstractlayers.degrees.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.abstractlayers.degrees.ds.Node;

public class FileUtils {
	public void readFile ( String fileName){
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(fileName));
			while ((sCurrentLine = br.readLine()) != null) {
				System.out.println(sCurrentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public List<String> getLinesFromFile(String fileName) throws Exception{
		List<String> lines = new ArrayList<String>();
		BufferedReader br = null;
	    String sCurrentLine;
		br = new BufferedReader(new FileReader(fileName));
		while ((sCurrentLine = br.readLine()) != null) {
				lines.add(sCurrentLine);
		 }
		if (br != null)br.close();
		return lines;
	}
	
	public void writeContentToFile(String content , String directory, String fileName) throws IOException{
		 File file = new File(directory+fileName);
		  if (!file.exists()) {
				file.createNewFile();
			}
		  
		  FileWriter fw = new FileWriter(file.getAbsoluteFile());
		  BufferedWriter bw = new BufferedWriter(fw);
		  bw.write(content);
		  bw.close();
	}
}
