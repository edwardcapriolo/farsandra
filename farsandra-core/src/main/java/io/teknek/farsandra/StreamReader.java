package io.teknek.farsandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class StreamReader implements Runnable {

  private InputStream is;
  private List<LineHandler> handlers;
  
  public StreamReader(InputStream is){
    this.is = is;
    handlers = new ArrayList<LineHandler>();
  }
  
  public void addHandler(LineHandler handler){
    handlers.add(handler);
  } 
  
  @Override
  public void run() {    
    String line = null;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))){
      while ((line = br.readLine()) != null){
        for (LineHandler h: handlers){
          h.handleLine(line);
        }
      }
    } catch (IOException e) {
      System.out.println("Connection to Cassandra closed: " + e.getMessage());
    } 
  }
  
}