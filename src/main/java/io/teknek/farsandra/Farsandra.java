package io.teknek.farsandra;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;

public class Farsandra {

  private String version;
  private String host;
  private int port;
  private String instanceName;
  private boolean cleanInstanceOnStart;
  
  public Farsandra(){
    
  }
  
  public Farsandra withCleanInstanceOnStart(boolean start){
    this.cleanInstanceOnStart = start;
    return this;
  }
  
  public Farsandra withVersion(String version){
    this.version = version;
    return this;
  }
  
  public Farsandra withPort(int port){
    this.port = port;
    return this;
  }
  
  public Farsandra withHost(String host){
      this.host = host;
      return this;
  }
  
  public Farsandra withInstanceName(String name){
    this.instanceName = name;
    return this;
  }
  
  public void start(){
    String userHome  = System.getProperty("user.home");
    File home = new File(userHome);
    if (!home.exists()){
      throw new RuntimeException("could not find your home " + home);
    }
    File farsandra = new File(home, ".farsandra");
    if (!farsandra.exists()){
      boolean result = farsandra.mkdir();
      if (!result){
        throw new RuntimeException("could not create " + farsandra);
      }
    }
    String gunzip = "apache-cassandra-"+version+"-bin.tar.gz";
    File archive = new File(farsandra, gunzip); 
    if (!archive.exists()){
      //web fetch
      //http://archive.apache.org/dist/cassandra/2.0.4/apache-cassandra-2.0.4-bin.tar.gz
      //extract with common compress
    }
    File cRoot = new File(farsandra, "apache-cassandra-"+version);
    if (!cRoot.exists()){
      throw new RuntimeException("could not find root dir " + cRoot);
    }
    //#   JVM_OPTS -- Additional arguments to the JVM for heap size, etc
    //#   CASSANDRA_CONF -- Directory containing Cassandra configuration files.
    //String yarn = " -Dcassandra-foreground=yes org.apache.cassandra.service.CassandraDaemon";
    
    File instanceBase = new File(instanceName);
    if (this.cleanInstanceOnStart){
      deleteRecursive(instanceBase);
    }
    instanceBase.mkdir();
    File instanceLog = new File(instanceBase, "log");
    instanceLog.mkdir();
    File instanceData = new File(instanceBase, "data");
    instanceData.mkdir();
    File instanceConf = new File(instanceBase, "conf");
    instanceConf.mkdir();
    copyConfToInstanceDir(cRoot , instanceConf);
  }
  
  public static void copyConfToInstanceDir(File cassandraBinaryRoot, File instanceConf){
    File binaryConf = new File ( cassandraBinaryRoot, "conf");
    for (File file: binaryConf.listFiles()){
      if (!file.getName().equals("cassandra.yaml")){
        try {
          Files.copy(file.toPath(), new File(instanceConf,file.getName()).toPath() );
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
      }
    }
  }
  public static void deleteRecursive(File baseDir){
    
  }
  public static void main (String [] args){
    
  }
}
