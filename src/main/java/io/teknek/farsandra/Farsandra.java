package io.teknek.farsandra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Farsandra {

  private String version;
  private String host;
  private Integer rpcPort;
  private Integer storagePort;
  private String instanceName;
  private boolean cleanInstanceOnStart;
  private boolean createConfigurationFiles;
  private List<String> seeds;
  private CForgroundManager manager;
  
  public Farsandra(){
    manager = new CForgroundManager();
  }
  
  public Farsandra withCleanInstanceOnStart(boolean start){
    this.cleanInstanceOnStart = start;
    return this;
  }
  
  public Farsandra withSeeds(List<String> seeds){
    this.seeds = seeds;
    return this;
  }
  
  public Farsandra withVersion(String version){
    this.version = version;
    return this;
  }
  
  public Farsandra withPort(int port){
    this.rpcPort = port;
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
  
  public Farsandra withCreateConfigurationFiles(boolean write){
    this.createConfigurationFiles = write;
    return this;
  }
    
  /**
   * Starts the instance of cassandra in a non-blocking manner. Use line handler and other methods
   * to detect when startup is complete.
   */
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
    if (cleanInstanceOnStart){
      delete(instanceBase);
    }
    File instanceConf = new File(instanceBase, "conf");
    if (createConfigurationFiles){ 
      instanceBase.mkdir();
      File instanceLog = new File(instanceBase, "log");
      instanceLog.mkdir();
      File instanceData = new File(instanceBase, "data");
      instanceData.mkdir();
      instanceConf.mkdir();
      copyConfToInstanceDir(cRoot , instanceConf);
      File binaryConf = new File(cRoot, "conf");
      File cassandraYaml = new File (binaryConf,"cassandra.yaml");
      List<String> lines;
      try {
        lines = Files.readAllLines(cassandraYaml.toPath(), Charset.defaultCharset());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      lines = replaceHost(lines);
      lines = replaceThisWithThatExpectNMatch(lines, 
              "    - /var/lib/cassandra/data", 
              "    - " + this.instanceName + "/data/data" , 1);
      lines = replaceThisWithThatExpectNMatch(lines, 
              "listen_address: localhost", 
              "listen_address: " +host , 1);
      lines = replaceThisWithThatExpectNMatch(lines, 
              "commitlog_directory: /var/lib/cassandra/commitlog", 
              "commitlog_directory: " + this.instanceName + "/data/commitlog" , 1 );
      lines = replaceThisWithThatExpectNMatch(lines,
              "saved_caches_directory: /var/lib/cassandra/saved_caches", 
              "saved_caches_directory: " + this.instanceName + "/data/saved_caches", 1);
      if (storagePort != null){
        lines = replaceThisWithThatExpectNMatch(lines, "storage_port: 7000", "storage_port: "+storagePort, 1 );
      }
      if (rpcPort != null){
        lines = replaceThisWithThatExpectNMatch(lines, "rpc_port: 9160", "rpc_port: " + rpcPort, 1);
      }
      if (seeds != null) {
        lines = replaceThisWithThatExpectNMatch(lines, "          - seeds: \"127.0.0.1\"",
                "         - seeds: \"" + seeds.get(0) + "\"", 1);
      }
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(
                new File(instanceConf, "cassandra.yaml")))){
        for (String s: lines){
          bw.write(s);
          bw.newLine();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } 
    }
     //  /bin/bash -c "env - X=5 y=2 sh xandy.sh"
    //#   JVM_OPTS -- Additional arguments to the JVM for heap size, etc
    //#   CASSANDRA_CONF -- Directory containing Cassandra configuration files.
    File cstart = new File(new File( cRoot, "bin"),"cassandra");
    
    /*String launch = "/bin/bash -c \"/usr/bin/env - CASSANDRA_CONF=" + instanceConf.getAbsolutePath() +" JAVA_HOME="+
            "/usr/java/jdk1.7.0_45 "
            + cstart.getAbsolutePath().toString() + " -f \""; */
    String [] launchArray = new String [] { "/bin/bash" , "-c" , "/usr/bin/env - CASSANDRA_CONF=" + instanceConf.getAbsolutePath() +" JAVA_HOME="+
            "/usr/java/jdk1.7.0_45 "
            + cstart.getAbsolutePath().toString() + " -f " };
    manager.setLaunchArray(launchArray);
    manager.go();
  }
  
  void delete(File f)  {
    if (f.isDirectory()) {
      for (File c : f.listFiles())
        delete(c);
    }
    if (!f.delete())
      throw new RuntimeException("Failed to delete file: " + f);
  }
  
  public List<String> replaceThisWithThatExpectNMatch(List<String> lines, String match, String replace, int expectedMatches){
    List<String> result = new ArrayList<String>();
    int replaced = 0;
    for (String line: lines){
      if (!line.equals(match)){
        result.add(line);
      } else{
        replaced++;
        result.add(replace);
      }
    }
    if (replaced != expectedMatches){
      throw new RuntimeException("looking to make matches replacement but made "+replaced
              +" . Likely that farsandra does not understand this version ");
    }
    return result;
  }
  
 
  public List<String> replaceHost(List<String> lines){
    List<String> result = new ArrayList<String>();
    int replaced = 0;
    for (String line: lines){
      System.out.println(line);
      if (!line.contains("rpc_address: localhost")){
        result.add(line);
      } else{
        replaced++;
        result.add("rpc_address: "+host);
      }
    }
    if (replaced != 1){
      throw new RuntimeException("looking to make 1 replacement but made "+replaced
              +" . Likely that farsandra does not understand this version ");
    }
    return result;
  }
  
  public static void copyConfToInstanceDir(File cassandraBinaryRoot, File instanceConf){
    File binaryConf = new File(cassandraBinaryRoot, "conf");
    for (File file: binaryConf.listFiles()){  
      if (!file.getName().equals("cassandra.yaml")){
        try {
          Files.copy(file.toPath(), new File(instanceConf,file.getName()).toPath() );
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
    
  public CForgroundManager getManager() {
    return manager;
  }

  public void setManager(CForgroundManager manager) {
    this.manager = manager;
  }

}
