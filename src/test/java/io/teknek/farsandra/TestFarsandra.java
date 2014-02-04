  package io.teknek.farsandra;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;



import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestFarsandra {

  private Farsandra fs;
  
  @Before
  public void setup(){
    fs = new Farsandra();
  }
  
  @After
  public void close(){
    if (fs != null){
      try {
        fs.getManager().destroyAndWaitForShutdown(6);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  @Test
  public void testShutdownWithLatch() throws InterruptedException {
    fs.withVersion("2.0.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("3_1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("127.0.0.1");
    fs.withSeeds(Arrays.asList("127.0.0.1"));
    fs.withJmxPort(9999);   
    fs.appendLineToYaml("#this means nothing");
    fs.appendLinesToEnv("#this also does nothing");
    fs.withEnvReplacement("#MALLOC_ARENA_MAX=4", "#MALLOC_ARENA_MAX=wombat");
    fs.withYamlReplacement("# NOTE:", "# deNOTE:");
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      } 
    );
    fs.start();
    started.await(10, TimeUnit.SECONDS);
  }
  
  @Ignore
  @Test
  public void threeNodeTest() throws InterruptedException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
    Farsandra fs = new Farsandra();
    {
    fs.withVersion("2.0.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("3_1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("127.0.0.1");
    fs.withSeeds(Arrays.asList("127.0.0.1"));
    fs.withJmxPort(9999);
    
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      } 
    );
    fs.start();
    started.await();
    }
    
    Farsandra fs2 = new Farsandra();
    {

      fs2.withVersion("2.0.4");
      fs2.withCleanInstanceOnStart(true);
      fs2.withInstanceName("3_2");
      fs2.withCreateConfigurationFiles(true);
      fs2.withHost("127.0.0.2");
      fs2.withSeeds(Arrays.asList("127.0.0.1"));
      fs2.withJmxPort(9998);

      final CountDownLatch started2 = new CountDownLatch(1);
      fs2.getManager().addOutLineHandler(new LineHandler() {
        @Override
        public void handleLine(String line) {
          System.out.println("out " + line);
          if (line.contains("Listening for thrift clients...")) {
            started2.countDown();
          }
        }
      });
      fs2.start();
      started2.await();
    }
    
    
    Farsandra fs3 = new Farsandra();
    {

      fs3.withVersion("2.0.4");
      fs3.withCleanInstanceOnStart(true);
      fs3.withInstanceName("3_3");
      fs3.withCreateConfigurationFiles(true);
      fs3.withHost("127.0.0.3");
      fs3.withSeeds(Arrays.asList("127.0.0.1"));
      fs3.withJmxPort(9997);

      final CountDownLatch started3 = new CountDownLatch(1);
      fs3.getManager().addOutLineHandler(new LineHandler() {
        @Override
        public void handleLine(String line) {
          System.out.println("out " + line);
          if (line.contains("Listening for thrift clients...")) {
            started3.countDown();
          }
        }
      });
      fs3.start();
      started3.await();
    }

    try {
      FramedConnWrapper wrap = new FramedConnWrapper("127.0.0.1", 9160);

      String ks = "CREATE KEYSPACE test "
              + " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 2} ";
      wrap.open();
      wrap.getClient().execute_cql3_query(ByteBuffer.wrap((ks).getBytes()), Compression.NONE,
              ConsistencyLevel.ALL);
      wrap.getClient().set_keyspace("test");
      wrap.getClient().execute_cql3_query(
              ByteBuffer.wrap(("CREATE TABLE userstats ( " + " username varchar, "
                      + " countername varchar, " + " value counter, "
                      + " PRIMARY KEY (username, countername) " + " ) with compact storage  ")
                      .getBytes()), Compression.NONE, ConsistencyLevel.ALL);

      Thread.sleep(10000);
      String incr = "UPDATE userstats  " + " SET value = value + 1 "
              + " WHERE username = 'ed' and countername= 'friends' ";
      for (int i = 0; i < 10000; i++) {
        wrap.getClient().execute_cql3_query(ByteBuffer.wrap((incr).getBytes()), Compression.NONE,
                ConsistencyLevel.ONE);
      }
      fs2.getManager().destroy();
      for (int i = 0; i < 10000; i++) {
        wrap.getClient().execute_cql3_query(ByteBuffer.wrap((incr).getBytes()), Compression.NONE,
                ConsistencyLevel.ONE);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    System.out.println("added");
    fs2.withCleanInstanceOnStart(false);
    fs2.withCreateConfigurationFiles(false);
    fs2.start();
    Thread.sleep(100000);
    fs.getManager().destroy();
    fs3.getManager().destroy();
  }
  
  @Test
  public void simpleOtherTest() throws InterruptedException{
    fs.withVersion("2.0.3");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("localhost");
    fs.withSeeds(Arrays.asList("localhost"));
    fs.withJmxPort(9999);
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      } 
    );
    
    fs.getManager().addErrLineHandler( new LineHandler(){
      @Override
      public void handleLine(String line) {
        System.out.println("err "+line);
      }
    });
    fs.start();
    started.await();
    System.out.println("Thrift open. Party time!");
    Thread.sleep(10000);
  }
  
  @Test
  public void simpleTest() throws InterruptedException {
    fs.withVersion("2.0.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("localhost");
    fs.withSeeds(Arrays.asList("localhost"));
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      } 
    );
    fs.getManager().addErrLineHandler( new LineHandler(){
      @Override
      public void handleLine(String line) {
        System.out.println("err "+line);
      }
    });
    fs.start();
    started.await();
    System.out.println("Thrift open. Party time!");
    Thread.sleep(10000);
  }
}
