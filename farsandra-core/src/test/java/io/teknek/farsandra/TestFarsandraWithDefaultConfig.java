package io.teknek.farsandra;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestFarsandraWithDefaultConfig {

  private Farsandra fs;
  
  @Before
  public void setup(){
    fs = new Farsandra(true);
  }
  
  @After
  public void close(){
    if (fs != null){
      try {
        fs.getManager().destroyAndWaitForShutdown(6);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  @Test
  public void testShutdownWithLatch() throws InterruptedException {
    fs.withVersion("2.2.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("target/3_1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("127.0.0.1");
    fs.withSeeds(Arrays.asList("127.0.0.1"));
    fs.withJmxPort(9999);   
    fs.appendLineToYaml("#this means nothing");
    fs.appendLinesToEnv("#this also does nothing");
    fs.withEnvReplacement("# Per-thread stack size.", "# Per-thread stack size. wombat");
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
    fs.getManager().addProcessHandler(new ProcessHandler() { 
      @Override
      public void handleTermination(int exitValue) {
        System.out.println("Cassandra terminated with exit value: " + exitValue);
        started.countDown();
      }
    });
    fs.start();
    started.await(10, TimeUnit.SECONDS);
    assertTrue("Cassandra is not running", fs.getManager().isRunning());
    fs.getManager().destroyAndWaitForShutdown(10);
  }
  
  @Test
  public void simpleTest() throws InterruptedException {
    fs.withVersion("2.2.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("target/1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("localhost");
    fs.withSeeds(Arrays.asList("localhost"));
    final CountDownLatch started = new CountDownLatch(1);
    final AtomicBoolean thriftOpen = new AtomicBoolean(false);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
            thriftOpen.set(true);
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
    fs.getManager().addProcessHandler(new ProcessHandler() { 
      @Override
      public void handleTermination(int exitValue) {
        System.out.println("Cassandra terminated with exit value: " + exitValue);
        started.countDown();
      }
    });
    fs.start();
    started.await();
    assertTrue("Thrift didn't started", thriftOpen.get());
    assertTrue("Cassandra is not running", fs.getManager().isRunning());
    System.out.println("Thrift open. Party time!");
    fs.getManager().destroyAndWaitForShutdown(10);
  }
}
