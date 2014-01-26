package io.teknek.farsandra;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import org.apache.log4j.Logger;

public class Farsandra {

  private static Logger LOGGER = Logger.getLogger(Farsandra.class);
  private String version;
  private String host;
  private Integer rpcPort;
  private Integer storagePort;
  private String instanceName;
  private boolean cleanInstanceOnStart;
  private boolean createConfigurationFiles;
  private List<String> seeds;
  private CForgroundManager manager;
  private String javaHome;
  private Integer jmxPort;
  private String maxHeapSize = "256M";
  private String heapNewSize = "100M";
  
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
  
  /**
   * Set the version of cassandra. Should be a string like "2.0.4"
   * @param version
   * @return
   */
  public Farsandra withVersion(String version){
    this.version = version;
    return this;
  }
  
  /**
   * Sets the RPC port
   * @param port
   * @return
   */
  public Farsandra withPort(int port){
    this.rpcPort = port;
    return this;
  }
  
  /**
   * sets the listen host and the rpc host
   * @param host
   * @return
   */
  public Farsandra withHost(String host){
      this.host = host;
      return this;
  }
  
  /**
   * Sets the instance name. This will also be the data directory where the instance is found
   * @param name
   * @return
   */
  public Farsandra withInstanceName(String name){
    this.instanceName = name;
    return this;
  }
  
  public Farsandra withCreateConfigurationFiles(boolean write){
    this.createConfigurationFiles = write;
    return this;
  }
  
  public Farsandra withJavaHome(String javaHome){
    this.javaHome = javaHome;
    return this;
  }
  
  public Farsandra withJmxPort(int jmxPort){
    this.jmxPort = jmxPort;
    return this;
  }
  
  public static void uncompressTarGZ(File tarFile, File dest) throws IOException {
    // http://stackoverflow.com/questions/11431143/how-to-untar-a-tar-file-using-apache-commons/14211580#14211580
    dest.mkdir();
    TarArchiveInputStream tarIn = null;
    tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(
            new FileInputStream(tarFile))));
    TarArchiveEntry tarEntry = tarIn.getNextTarEntry();
    while (tarEntry != null) {// create a file with the same name as the tarEntry
      File destPath = new File(dest, tarEntry.getName());
      if (destPath.getName().equals("cassandra")){
        destPath.setExecutable(true);
      }
      if (tarEntry.isDirectory()) {
        destPath.mkdirs();
      } else {
        destPath.createNewFile();
        byte[] btoRead = new byte[1024];
        BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(destPath));
        int len = 0;
        while ((len = tarIn.read(btoRead)) != -1) {
          bout.write(btoRead, 0, len);
        }
        bout.close();
        btoRead = null;
      }
      tarEntry = tarIn.getNextTarEntry();
    }
    tarIn.close();
  }
  
  public void download(String version, File location){
    LOGGER.info("Version of Cassandra not found locally. Attempting to fetch it from cloud");
    try {
      String file = "apache-cassandra-" + version + "-bin.tar.gz";
      URL url = new URL("http://archive.apache.org/dist/cassandra/" + version + "/" + file);
      URLConnection conn = url.openConnection();
      InputStream in = conn.getInputStream();
      FileOutputStream out = new FileOutputStream(new File(location, file));
      byte[] b = new byte[1024];
      int count;
      while ((count = in.read(b)) >= 0) {
        out.write(b, 0, count);
      }
      out.flush();
      out.close();
      in.close();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  
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
      download(version,farsandra);
      try {
        uncompressTarGZ(archive,farsandra);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
      if (instanceBase.exists()){
        delete(instanceBase);
      }
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
      File cassandraYaml = new File(binaryConf, "cassandra.yaml");
      makeCassandraEnv(binaryConf, instanceConf);
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
    String command = "/usr/bin/env - CASSANDRA_CONF=" + instanceConf.getAbsolutePath();
    //command = command + " JAVA_HOME=" + "/usr/java/jdk1.7.0_45 ";
    command = command + buildJavaHome()+ " ";
    command = command + " /bin/bash " + cstart.getAbsolutePath().toString() + " -f ";
    String [] launchArray = new String [] { 
            "/bin/bash" , 
            "-c" , 
             command };
    manager.setLaunchArray(launchArray);
    manager.go();
  }
  
  /**
   * Builds the cassandra-env.sh replacing stuff along the way
   * @param binaryConf directory of downloaded conf
   * @param instanceConf directory for conf to be generated
   */
  private void makeCassandraEnv(File binaryConf, File instanceConf) {
    String envFile = "cassandra-env.sh";
    File cassandraYaml = new File(binaryConf, envFile);
    List<String> lines;
    try {
      lines = Files.readAllLines(cassandraYaml.toPath(), Charset.defaultCharset());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (jmxPort != null){
      lines = replaceThisWithThatExpectNMatch(lines, "JMX_PORT=\"7199\"", "JMX_PORT=\""
            + this.jmxPort + "\"", 1);
    }
    if (maxHeapSize !=null){
      lines = replaceThisWithThatExpectNMatch(lines, "#MAX_HEAP_SIZE=\"4G\"", 
              "MAX_HEAP_SIZE=\""+this.maxHeapSize+"\"", 1);
    }
    if (heapNewSize != null) {
      lines = replaceThisWithThatExpectNMatch(lines, "#HEAP_NEWSIZE=\"800M\"", "HEAP_NEWSIZE=\""
              + heapNewSize + "\"", 1);
    }
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(instanceConf, envFile)))) {
      for (String s : lines) {
        bw.write(s);
        bw.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public String buildJavaHome() {
    if (this.javaHome != null) {
      return " JAVA_HOME=" + this.javaHome;
    } else if (System.getenv("JAVA_HOME") != null) {
      return " JAVA_HOME=" + System.getenv("JAVA_HOME");
    } else {
      return "";
    }
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
      if (!file.getName().equals("cassandra.yaml") || 
              !file.getName().equals("cassandra-env.sh")){
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
