package io.teknek.farsandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
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
  private String [] envArray = null;
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
  private List<ProcessHandler> processHandlers = new ArrayList<ProcessHandler>();
  
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
   * Set the fork command environment variables

   * @param envArray
   */
  public void setEvnArray(String[] envArray) {
    this.envArray = envArray;
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
   * Add a handler for cassandra process events
   * 
   * @param h
   */
  public void addProcessHandler(ProcessHandler h){
    processHandlers.add(h);
  }

  /**
   * Start the process and attach handlers
   */
  public void go() {
    /*
     * String launch = "/bin/bash -c \"env - CASSANDRA_CONF=" + instanceConf.getAbsolutePath()
     * +" JAVA_HOME="+ "/usr/java/jdk1.7.0_45 " + cstart.getAbsolutePath().toString() + " -f \"";
     */
    LOGGER.debug("ENVIRONMENT: " + Arrays.asList(this.envArray));
    LOGGER.debug("LAUNCH COMANDS: " + Arrays.asList(this.launchArray));
    Runtime rt = Runtime.getRuntime();
    try {
        if (envArray != null)
        {
            process = rt.exec(launchArray,envArray);
        }
        else
        {
            process = rt.exec(launchArray);
        }
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
          for (ProcessHandler h : processHandlers) {
            h.handleTermination(exitValue.get());
          }
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
    if (isWindows()) {
      
      // Unlike in unix killing a process does not kill any child processes.
      // In order to destroy our Cassandra java process, we'll use wimc to query
      // for a java process with our instance name in the classpath.
      String confPath = null;
      for (String s : envArray)
      {
        if (s.contains("CASSANDRA_CONF")) {
          confPath = s.split("=")[1];
          
          // wimc requires paths to be double-quoted backslashes.
          String wmicKill = "wmic PROCESS where " +
                            "\"" + "name like '%java%' and CommandLine like " +
                              "'%" + confPath.replace("\\", "\\\\") + "%' and " +
                              "CommandLine like '%CassandraDaemon%'" + "\"" +
                            " call Terminate";
          try {
            Process p = Runtime.getRuntime().exec(wmicKill);
            p.getOutputStream().close();
            int exitCode = p.exitValue();
            if (exitCode == 0) {
              LOGGER.info("Cassandra process destroyed.");
            }else {
              LOGGER.error("Non-zero exit code from killing the cassandra process.");
            }
            
          } catch (IOException e) {
            LOGGER.error("Could not kill the Cassandra java child process");
          }
          break;
        } 
      }
      if (confPath == null) {
        LOGGER.error("Could not locate and kill java child process");
      }
      
      process.destroy();
      try {
        process.waitFor();
      } catch (InterruptedException e) {
        ; // nothing to do
      }
    } else {
      process.destroy();
    }
  }
  /**
   * Determines if the operating system is Windows
   * 
   * @return a boolean value indicating if we're on Windows OS
   */
  
  private boolean isWindows() {
    String os = System.getProperty("os.name");
    return os.contains("Windows");
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

  /**
   * Return a boolean value indicating if the process is running
   * 
   * @return a boolean value indicating if the process is running
   */
  public boolean isRunning() {
    return waitForShutdown.getCount() > 0;
  }

  /**
   * Returns the exit value of the Cassandra process.
   *
   * @return the exit value
   * @throws IllegalThreadStateException
   *           if the process has not yet terminated
   */
  public int getExitValue() {
    return process.exitValue();
  }

  /**
   * Wait for the Cassandra process termination.

   * @return the exit value of the Cassandra process
   * @throws InterruptedException
   *           if the current thread is {@linkplain Thread#interrupt()
   *           interrupted} by another thread while it is waiting, then the wait
   *           is ended and an {@link InterruptedException} is thrown.
   */
  public int waitFor() throws InterruptedException {
    waitForShutdown.await();
    return exitValue.get();
  }
}