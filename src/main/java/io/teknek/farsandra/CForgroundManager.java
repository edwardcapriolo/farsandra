package io.teknek.farsandra;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CForgroundManager {
  //private String launch;
  private String [] launchArray;
  private Process p; 
  private AtomicInteger x = new AtomicInteger();
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

  
  public void setLaunchArray(String[] launchArray) {
    this.launchArray = launchArray;
  }

/*
  public String getLaunch() {
    return launch;
  }
  */

  /*
  public void setLaunch(String launch) {
    this.launch = launch;
  }*/

  public void addOutLineHandler(LineHandler h){
    out.add(h);
  }
  
  public void addErrLineHandler(LineHandler h){
    err.add(h);
  }
  
  public void go() {
    /*
     * String launch = "/bin/bash -c \"env - CASSANDRA_CONF=" + instanceConf.getAbsolutePath()
     * +" JAVA_HOME="+ "/usr/java/jdk1.7.0_45 " + cstart.getAbsolutePath().toString() + " -f \"";
     */
    System.out.println(Arrays.asList(this.launchArray));
    Runtime rt = Runtime.getRuntime();

    try {
      //p = rt.exec(launch);
      p = rt.exec(launchArray);

    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    InputStream output = p.getInputStream();
    InputStream error = p.getErrorStream();

    waitForTheEnd = new Thread() {
      public void run() {
        try {
          x.set(p.waitFor());
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
  
  public void destroy(){
    p.destroy();
  }
}