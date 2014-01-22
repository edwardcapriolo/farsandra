package io.teknek.farsandra;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Launch a process in the foreground get events from the processes output/error streams
 * @author edward
 *
 */
public class CForgroundManager {

  private String [] launchArray;
  private Process process; 
  private AtomicInteger exitValue = new AtomicInteger(-9999);
  private Thread waitForTheEnd;
  private Thread outstreamThread;
  private Thread errstreamThread;
  
  private List<LineHandler> out = new ArrayList<LineHandler>();
  private List<LineHandler> err = new ArrayList<LineHandler>();
  
  public CForgroundManager(){
    
  }
  
  public String[] getLaunchArray() {
    return launchArray;
  }

  /**
   * Set the fork command 
   * @param launchArray
   */
  public void setLaunchArray(String[] launchArray) {
    this.launchArray = launchArray;
  }

  /**
   * Add a handler for system out events
   * @param h
   */
  public void addOutLineHandler(LineHandler h){
    out.add(h);
  }
  
  
  /**
   * Add a handler for system error events
   * @param h
   */
  public void addErrLineHandler(LineHandler h){
    err.add(h);
  }
  
  /**
   * Start the process and attach handlers
   */
  public void go() {
    /*
     * String launch = "/bin/bash -c \"env - CASSANDRA_CONF=" + instanceConf.getAbsolutePath()
     * +" JAVA_HOME="+ "/usr/java/jdk1.7.0_45 " + cstart.getAbsolutePath().toString() + " -f \"";
     */
    System.out.println(Arrays.asList(this.launchArray));
    Runtime rt = Runtime.getRuntime();
    try {
      process = rt.exec(launchArray);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    InputStream output = process.getInputStream();
    InputStream error = process.getErrorStream();
    waitForTheEnd = new Thread() {
      public void run() {
        try {
          exitValue.set(process.waitFor());
        } catch (InterruptedException e) {
        }
      }
    };
    StreamReader outReader = new StreamReader(output);
    for (LineHandler h : this.out){
      outReader.addHandler(h);
    }
    outstreamThread = new Thread(outReader);
    StreamReader errReader = new StreamReader(error);
    for (LineHandler h: this.err){
      errReader.addHandler(h);
    }
    errstreamThread = new Thread(errReader);
    waitForTheEnd.start();
    outstreamThread.start();
    errstreamThread.start();
  }
  
  /**
   * End the process
   */
  public void destroy(){
    process.destroy();
  }
}