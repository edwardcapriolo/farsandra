package io.teknek.farsandra;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

public class TestFarsandra {

  
  @Test
  public void threeNodeTest() throws InterruptedException {
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
    fs2.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started2.countDown();
          }
        }
      } 
    );
    fs2.start();
    started2.await();
    }
    
    Thread.sleep(10000);
    fs.getManager().destroy();
    fs2.getManager().destroy();
  }
  
  @Test
  public void simpleOtherTest() throws InterruptedException{
    Farsandra fs = new Farsandra();
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
    fs.getManager().destroy();
  }
  
  
  @Test
  public void simpleTest() throws InterruptedException{
    Farsandra fs = new Farsandra();
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
    fs.getManager().destroy();
  }
}
