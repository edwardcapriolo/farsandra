package io.teknek.farsandra;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import io.teknek.farsandra.config.ConfigHolder;

public class Farsandra {

  private static Logger LOGGER = Logger.getLogger(Farsandra.class);
  private String version;
  private String host;
  private Integer rpcPort;
  private Integer storagePort;
  private Integer nativeTransportPort;
  private String instanceName;
  private boolean cleanInstanceOnStart;
  private boolean createConfigurationFiles;
  private List<String> seeds;
  private CForgroundManager manager;
  private String javaHome;
  private Integer jmxPort;
  private String maxHeapSize = "256M";
  private String heapNewSize = "100M";
  private List<String> yamlLinesToAppend;
  private List<String> envLinesToAppend;
  private Map<String ,String> envReplacements;
  private Map<String, String> yamlReplacements;
  private ConfigHolder configHolder;

  public Farsandra(){
    manager = new CForgroundManager();
    yamlLinesToAppend = new ArrayList<String>();
    envLinesToAppend = new ArrayList<String>();
    envReplacements = new TreeMap<String, String>();
    yamlReplacements = new TreeMap<String, String>();
  }

  public Farsandra(boolean readProps){
    manager = new CForgroundManager();
    yamlLinesToAppend = new ArrayList<String>();
    envLinesToAppend = new ArrayList<String>();
    envReplacements = new TreeMap<String, String>();
    yamlReplacements = new TreeMap<String, String>();
    if (readProps == true) configHolder = new ConfigHolder();
  }


  public Farsandra(String configFile){
    manager = new CForgroundManager();
    yamlLinesToAppend = new ArrayList<String>();
    envLinesToAppend = new ArrayList<String>();
    envReplacements = new TreeMap<String, String>();
    yamlReplacements = new TreeMap<String, String>();
    configHolder = new ConfigHolder(configFile);
   }

  public Farsandra withCleanInstanceOnStart(boolean start){
    this.cleanInstanceOnStart = start;
    return this;
  }

  public Farsandra withSeeds(List<String> seeds){
    this.seeds = seeds;
    return this;
  }

  public List<String> appendLinesToEnv(String line){
    envLinesToAppend.add(line);
    return envLinesToAppend;
  }

  /**
   * Add line to the bottom of yaml. Does not check if line already exists
   * @param line a line to add
   * @return the list of lines
   */
  public List<String> appendLineToYaml(String line){
    yamlLinesToAppend.add(line);
    return yamlLinesToAppend;
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
//  public Farsandra withYamlReplacement(String match, String replace){
//    this.yamlReplacements.put(match, replace);
//    return this;
//  }
  public Farsandra withEnvReplacement(String match,String replace){
    this.envReplacements.put(match, replace);
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
   * Sets the storage port
   * @param port
   * @return
   */
  public Farsandra withStoragePort(int port){
    this.storagePort = port;
    return this;
  }
  /**
   * Sets the CQL native transport port
   * @param port
   * @return
   */
  public Farsandra withNativeTransportPort(int port){
    this.nativeTransportPort = port;
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
      String file;
      URL url;
      if (configHolder == null){
        file = "apache-cassandra-" + version + "-bin.tar.gz";
        url = new URL("http://archive.apache.org/dist/cassandra/" + version + "/" + file);
      } else {
        file = this.getConfigHolder().getProperties().getProperty("cassandra.package.name.prefix") + version + this.getConfigHolder().getProperties().getProperty("cassandra.package.name.suffix");
        url = new URL(this.getConfigHolder().getProperties().getProperty("cassandra.package.dist.site") + version + "/" + file);
      }
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
    File farsandra;
    if (configHolder == null){
      farsandra = new File(home, ".farsandra");
    } else {
      farsandra = new File(home, this.getConfigHolder().getProperties().getProperty("farsandra.home.folder"));
    }
    if (!farsandra.exists()){
      boolean result = farsandra.mkdir();
      if (!result){
        throw new RuntimeException("could not create " + farsandra);
      }
    }
    String gunzip;
    if (configHolder == null){
      gunzip = "apache-cassandra-"+version+"-bin.tar.gz";
    } else {
      gunzip = this.getConfigHolder().getProperties().getProperty("cassandra.package.name.prefix")+version+this.getConfigHolder().getProperties().getProperty("cassandra.package.name.suffix");
    }
    File archive = new File(farsandra, gunzip);
    if (!archive.exists()){
      download(version,farsandra);
      try {
        uncompressTarGZ(archive,farsandra);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    File cRoot;
    if (configHolder == null){
      cRoot = new File(farsandra, "apache-cassandra-"+version);
    } else {
      cRoot = new File(farsandra, this.getConfigHolder().getProperties().getProperty("cassandra.package.name.prefix") + version);
    }
    if (!cRoot.exists()){
      boolean create = cRoot.mkdirs();
      if (!create){
        throw new RuntimeException("could not find root dir " + cRoot);
      }
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

    if (createConfigurationFiles){
      instanceBase.mkdir();
      File instanceConf;
      File instanceLog;
      File instanceData;
      if (configHolder == null){
        instanceConf = new File(instanceBase, "conf");
      } else {
        instanceConf = new File(instanceBase, this.getConfigHolder().getProperties().getProperty("farsandra.conf.dir"));
      }
      instanceConf.mkdir();
      if (configHolder == null){
        instanceLog = new File(instanceBase, "log");
      } else {
        instanceLog = new File(instanceBase, this.getConfigHolder().getProperties().getProperty("farsandra.log.dir"));
      }
      instanceLog.mkdir();
      if (configHolder == null){
        instanceData = new File(instanceBase, "data");
      } else {
        instanceData = new File(instanceBase, this.getConfigHolder().getProperties().getProperty("farsandra.data.dir"));
      }
      instanceData.mkdir();
      copyConfToInstanceDir(cRoot, instanceConf, configHolder);
      File binaryConf;
      File cassandraYaml;
      if (configHolder == null){
        binaryConf = new File(cRoot, "conf");
        cassandraYaml = new File(binaryConf, "cassandra.yaml");
      } else {
        binaryConf = new File(cRoot, this.getConfigHolder().getProperties().getProperty("farsandra.conf.dir"));
        cassandraYaml = new File(binaryConf, this.getConfigHolder().getProperties().getProperty("cassandra.config.file.name"));
      }

      setUpLoggingConf(instanceConf, instanceLog);
      makeCassandraEnv(binaryConf, instanceConf);

      try {
        Yaml yaml = new Yaml();
        @SuppressWarnings("unchecked")
        CassandraYaml cyaml = new CassandraYaml((LinkedHashMap<String,Object>)yaml.load(new FileReader(cassandraYaml)));
        cyaml.setRpc_address(host);
        ArrayList<String> dfd = new ArrayList<String>();
        dfd.add(this.instanceName + "/data/data");
        cyaml.setData_file_directories(dfd);
        cyaml.setListen_address(host);
        cyaml.setCommitlog_directory(this.instanceName + "/data/commitlog");
        cyaml.setSaved_caches_directory(this.instanceName + "/data/saved_caches");
        cyaml.setStart_rpc(true);
        if (storagePort != null){
          cyaml.setStorage_port(storagePort);
        }
        if (rpcPort != null){
          cyaml.setRpc_port(rpcPort);
        }
        if (nativeTransportPort != null){
          cyaml.setNative_transport_port(nativeTransportPort);
        }
        if (seeds != null) {
          cyaml.getSeed_provider().get(0).getParameters().get(0).setSeeds(seeds.get(0));
        }
//        for (Map.Entry<String,String> entry: yamlReplacements.entrySet()){
//          lines = replaceThisWithThatExpectNMatch(lines, entry.getKey(), entry.getValue(), 1);
//        }
        File instanceConfToWrite;
        if (configHolder == null){
          instanceConfToWrite = new File(instanceConf, "cassandra.yaml");
        } else {
          instanceConfToWrite = new File(instanceConf, this.getConfigHolder().getProperties().getProperty("cassandra.config.file.name"));
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(instanceConfToWrite));
        yaml.dump(cyaml.getMap(), bw);
        for (String line : yamlLinesToAppend) {
          bw.write(line);
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
    File instanceConf;
    if (configHolder == null){
      instanceConf = new File(instanceBase, "conf");
    } else {
      instanceConf = new File(instanceBase, this.getConfigHolder().getProperties().getProperty("farsandra.conf.dir"));
    }
    String command = "/usr/bin/env - CASSANDRA_CONF=" + instanceConf.getAbsolutePath();
    //command = command + " JAVA_HOME=" + "/usr/java/jdk1.7.0_45 ";
    command = command + buildJavaHome() + " ";
    command = command + " /bin/bash " + cstart.getAbsolutePath().toString() + " -f ";
    String [] launchArray = new String [] {
            "/bin/bash" ,
            "-c" ,
             command };
    manager.setLaunchArray(launchArray);
    manager.go();
  }



  /**
   * Replaces the default file path of the system log.
   * Cassandra comes with a log4j configuration that assumes, by default, there's a /var/log/cassandra directory
   * with R/W permissions.
   * While in a regular installation you have the chance to edit that file before starting Cassandra,
   * with Farsandra things are different: the instance is automatically started so if you don't have that directory
   * (or you didn't set those required permissions), an exception is printed out and the system.log is never created.
   * This method makes sure the default path of the logging file is replaced with a path located under the Farsandra instance
   * directory ($FARSANDRA_INSTANCE_DIR/log/system.log).
   *
   * @param instanceConfDirectory the instance "conf" directory.
   * @param instanceLogDirectory the instance "log" directory.
   */
  private void setUpLoggingConf(final File instanceConfDirectory,
    final File instanceLogDirectory) {
    final File log4ServerProperties = new File(instanceConfDirectory,
      "log4j-server.properties");
    final File systemLog = new File(instanceLogDirectory, "system.log");
    if (log4ServerProperties.exists()) {
      final String log4jAppenderConfLine = "log4j.appender.R.File";
      BufferedWriter writer = null;
      try {
        final List<String> lines = Files.readAllLines(
          log4ServerProperties.toPath(), Charset.defaultCharset());
        writer = new BufferedWriter(new FileWriter(log4ServerProperties));
        for (final String line : lines) {
          writer.write(
            (line.startsWith(log4jAppenderConfLine) ? log4jAppenderConfLine
              .concat("=").concat(systemLog.getAbsolutePath()) : line));
          writer.newLine();
        }
      } catch (final IOException exception) {
        throw new RuntimeException(exception);
      } finally {
        if (writer != null) {
          try {
            writer.close();
          } catch (final IOException ignore) {
            // Nothing to be done here...
          }
        }
      }
    } else {
      final File logbackXml = new File(instanceConfDirectory, "logback.xml");
      // Setting cassandra.logdir would be the clean way to do this, but that requires modifying bin/cassandra, so...
      final Pattern filePattern = Pattern.compile("\\s*\\Q<file>${cassandra.logdir}/\\E(?<fileName>[^<]*)\\Q</file>\\E");
      BufferedWriter writer = null;
      try {
        final List<String> lines = Files.readAllLines(
          logbackXml.toPath(), Charset.defaultCharset());
        writer = new BufferedWriter(new FileWriter(logbackXml));
        for (final String line : lines) {
          Matcher m = filePattern.matcher(line);
          if  (m.matches()) {
            File logFile = new File(instanceLogDirectory, m.group(1));
            writer.write("    <file>" + logFile.getAbsolutePath() + "</file>");
          } else {
            writer.write(line);
          }
          writer.newLine();
        }
      } catch (final IOException exception) {
        throw new RuntimeException(exception);
      } finally {
        if (writer != null) {
          try {
            writer.close();
          } catch (final IOException ignore) {
            // Nothing to be done here...
          }
        }
      }

    }
  }

  private List<String> envLinesToAppend(List<String> input){
    List<String> results = new ArrayList<String>();
    results.addAll( input);
    results.addAll(this.envLinesToAppend);
    return results;
  }

  /**
   * Builds the cassandra-env.sh replacing stuff along the way
   * @param binaryConf directory of downloaded conf
   * @param instanceConf directory for conf to be generated
   */
  private void makeCassandraEnv(File binaryConf, File instanceConf) {
    String envFile;
    if (configHolder == null){
      envFile = "cassandra-env.sh";
    } else {
      envFile = this.getConfigHolder().getProperties().getProperty("cassandra.environment.file.name");
    }
    File cassandraYaml = new File(binaryConf, envFile);
    List<String> lines;
    try {
      lines = Files.readAllLines(cassandraYaml.toPath(), Charset.defaultCharset());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (Map.Entry<String,String> entry: envReplacements.entrySet()){
      lines = replaceThisWithThatExpectNMatch(lines, entry.getKey(), entry.getValue(), 1);
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
    lines = envLinesToAppend(lines);
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
      throw new RuntimeException("looking to make " + expectedMatches +" of ('" + match + "')->'(" + replace + "') but made "+replaced
              +" . Likely that farsandra does not understand this version of configuration file. ");
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
      throw new RuntimeException("looking to make 1 replacement but made " + replaced
              +" . Likely that farsandra does not understand this version of configuration file");
    }
    return result;
  }

  public static void copyConfToInstanceDir(File cassandraBinaryRoot, File instanceConf, ConfigHolder configHolder){
    if (configHolder == null){
      File binaryConf = new File(cassandraBinaryRoot, "conf");
      for (File file: binaryConf.listFiles()){
        if (!file.getName().equals("cassandra.yaml") ||
              !file.getName().equals("cassandra-env.sh")){
          try {
            Files.copy(file.toPath(), new File(instanceConf, file.getName()).toPath() );
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    } else {
      File binaryConf = new File(cassandraBinaryRoot, configHolder.getProperties().getProperty("farsandra.conf.dir"));
      for (File file: binaryConf.listFiles()){
        if (!file.getName().equals(configHolder.getProperties().getProperty("cassandra.config.file.name")) ||
              !file.getName().equals(configHolder.getProperties().getProperty("cassandra.environment.file.name"))){
          try {
            Files.copy(file.toPath(), new File(instanceConf, file.getName()).toPath() );
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
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

  public ConfigHolder getConfigHolder() {
	return configHolder;
  }

  public void setConfigHolder(ConfigHolder configHolder) {
	this.configHolder = configHolder;
  }
  @SuppressWarnings("unchecked")
  public static class CassandraYaml {
    private LinkedHashMap<String, Object> map;

    public CassandraYaml(LinkedHashMap<String, Object> map) {
      this.map = map;
    }
    public LinkedHashMap<String, Object> getMap() {
      return map;
    }
    public String getRpc_address() {
      return (String)map.get("rpc_address");
    }
    public void setRpc_address(String rpc_address) {
      map.put("rpc_address", rpc_address);
    }
    public ArrayList<String> getData_file_directories() {
      return (ArrayList<String>)map.get("data_file_directories");
    }
    public void setData_file_directories(ArrayList<String> data_file_directories) {
      map.put("data_file_directories", data_file_directories);
    }
    public String getListen_address() {
      return (String)map.get("listen_address");
    }
    public void setListen_address(String listen_address) {
      map.put("listen_address", listen_address);
    }
    public String getCommitlog_directory() {
      return (String)map.get("commitlog_directory");
    }
    public void setCommitlog_directory(String commitlog_directory) {
      map.put("commitlog_directory", commitlog_directory);
    }
    public String getSaved_caches_directory() {
      return (String)map.get("saved_caches_directory");
    }
    public void setSaved_caches_directory(String saved_caches_directory) {
      map.put("saved_caches_directory", saved_caches_directory);
    }
    public Boolean getStart_rpc() {
      return (Boolean)map.get("start_rpc");
    }
    public void setStart_rpc(Boolean start_rpc) {
      map.put("start_rpc", start_rpc);
    }
    public Integer getStorage_port() {
      return (Integer)map.get("storage_port");
    }
    public void setStorage_port(Integer storage_port) {
      map.put("storage_port", storage_port);
    }
    public Integer getRpc_port() {
      return (Integer)map.get("rpc_port");
    }
    public void setRpc_port(Integer rpc_port) {
      map.put("rpc_port", rpc_port);
    }
    public Integer getNative_transport_port() {
      return (Integer)map.get("native_transport_port");
    }
    public void setNative_transport_port(Integer native_transport_port) {
      map.put("native_transport_port", native_transport_port);
    }
    public ArrayList<SeedProvider> getSeed_provider() {
      ArrayList<SeedProvider> ret = new ArrayList<SeedProvider>();
      ArrayList<LinkedHashMap<String,Object>> sps = ((ArrayList<LinkedHashMap<String,Object>>)map.get("seed_provider"));
      for (LinkedHashMap<String,Object> sp : sps) {
        ret.add(new SeedProvider(sp));
      }
      return ret;
    }
//    public void setSeed_provider(ArrayList<SeedProvider> seed_provider) {
//    }
    public void replace(String key, Object match, Object replace) {
      if (!map.containsKey(key)) {
        throw new RuntimeException("Unable to find key ('" + key + "'). "
          +"Likely that farsandra does not understand this version of configuration file. ");
      }
      if (!map.get(key).equals(match)) {
        throw new RuntimeException("Key ('" + key + "')'s value doesn't match expected value ('" + match + "'). "
          +"Likely that farsandra does not understand this version of configuration file. ");
      }
      map.put(key, replace);
    }
    static class SeedProvider {
      LinkedHashMap<String, Object> map;
      public SeedProvider(LinkedHashMap<String, Object> map) {
        this.map = map;
      }
      public LinkedHashMap<String, Object> getMap() {
        return map;
      }
      public String getClass_name() {
        return (String)map.get("class_name");
      }
      public void setClass_name(String class_name) {
        map.put("class_name", class_name);
      }
      public ArrayList<Parameter> getParameters() {
        ArrayList<Parameter> ret = new ArrayList<Parameter>();
        ArrayList<LinkedHashMap<String,Object>> ps = ((ArrayList<LinkedHashMap<String,Object>>)map.get("parameters"));
        for (LinkedHashMap<String,Object> p : ps) {
          ret.add(new Parameter(p));
        }
        return ret;

      }
      static class Parameter {
        LinkedHashMap<String, Object> map;

        public Parameter(LinkedHashMap<String, Object> map) {
          this.map = map;
        }
        public LinkedHashMap<String, Object> getMap() {
          return map;
        }
        public String getSeeds() {
          return (String)map.get("seeds");
        }
        public void setSeeds(String seeds) {
          map.put("seeds", seeds);
        }
      }
    }
  }
}
