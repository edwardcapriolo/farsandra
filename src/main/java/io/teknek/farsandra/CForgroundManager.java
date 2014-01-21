package io.teknek.farsandra;

import java.io.IOException;
import java.io.InputStream;

public class CForgroundManager {
  private String launch;
  private Process p; 
  private int x=-9999;
  private Thread waitForTheEnd;
  
  public void go(){
    /*
    String launch = "/bin/bash -c \"env - CASSANDRA_CONF=" + instanceConf.getAbsolutePath() +" JAVA_HOME="+
            "/usr/java/jdk1.7.0_45 "
            + cstart.getAbsolutePath().toString() + " -f \"";
            */
    System.out.println(launch);
    Runtime rt = Runtime.getRuntime();
 
      try {
        p = rt.exec(launch);
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      InputStream output = p.getInputStream();
      InputStream error = p.getErrorStream();
      
      waitForTheEnd = new Thread(){
        public void run(){
          try {
            x = p.waitFor();
          } catch (InterruptedException e) {
          }
        }
      };
      waitForTheEnd.start();
    }
  }
