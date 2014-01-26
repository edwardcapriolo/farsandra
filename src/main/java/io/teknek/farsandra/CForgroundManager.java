package io.teknek.farsandra;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

/**
 * Launch a process in the foreground get events from the processes output/error streams
 * @author edward
 *
 */
public class CForgroundManager {

  private static Logger LOGGER = Logger.getLogger(CForgroundManager.class);
  private String [] launchArray;
  private Process process; 
  /**
   * The exit value of the forked process. Initialized to 9999.
   */
  private AtomicInteger exitValue = new AtomicInteger(-9999);
  private Thread waitForTheEnd;
  private Thread outstreamThread;
  private Thread errstreamThread;
  private CountDownLatch waitForShutdown;
  
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
    LOGGER.debug(Arrays.asList(this.launchArray));
    Runtime rt = Runtime.getRuntime();
    try {
      process = rt.exec(launchArray);
    } catch (IOException e1) {
      LOGGER.error(e1.getMessage());
      throw new RuntimeException(e1);
    }
    waitForShutdown = new CountDownLatch(1);
    InputStream output = process.getInputStream();
    InputStream error = process.getErrorStream();
    waitForTheEnd = new Thread() {
      public void run() {
        try {
          exitValue.set(process.waitFor());
          waitForShutdown.countDown();
        } catch (InterruptedException e) {
          waitForShutdown.countDown();
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
   * End the process but do not wait for shutdown latch. Non blocking
   */
  public void destroy(){
    process.destroy();
  }
  
  /**
   * Wait a certain number of seconds for a shutdown. Throw up violently if it takes too long
   * @param seconds
   * @throws InterruptedException
   */
  public void destroyAndWaitForShutdown(int seconds) throws InterruptedException {
    if (waitForShutdown == null){
      throw new RuntimeException("Instance is not started. Can not shutdown.");
    }
    destroy();
    waitForShutdown.await(seconds, TimeUnit.SECONDS);
  }
}