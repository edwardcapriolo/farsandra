package io.teknek.farsandra;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class UnflushedJoinTest {

  Farsandra fs;
  Farsandra fs2;    
  Farsandra fs3;
  Farsandra fs4;
  
  public static  Farsandra startInstance(String version, final String instanceName, String listen, String seed,  int port) throws InterruptedException {
    Farsandra fs = new Farsandra();
    fs.withVersion(version);
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName(instanceName);
    fs.withCreateConfigurationFiles(true);
    fs.withHost(listen);
    fs.withSeeds(Arrays.asList(seed));
    fs.withJmxPort(port);
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler(new LineHandler() {
      @Override
      public void handleLine(String line) {
        System.out.println(instanceName+ " out " + line);
        if (line.contains("Listening for thrift clients...")) {
          started.countDown();
        }
      }
    });
    fs.start();
    started.await();
    return fs;
  }
 
  public void assertAllThere(String host) throws Exception {
    FramedConnWrapper wrap = new FramedConnWrapper(host, 9160);
    wrap.open();
    for (int i = 0; i < 10000; i++) {
      wrap.getClient().set_keyspace("test");
      String esQueueEl = "select * from userstats where username = '"+i+"'";
      System.out.println(esQueueEl);
      CqlResult r = wrap.getClient().execute_cql3_query(ByteBuffer.wrap(esQueueEl.getBytes()) , Compression.NONE, ConsistencyLevel.ONE);
      Assert.assertEquals(r.getRows().size(), 1);
    }
  }
  
  @Test
  public void threeNodeTest() throws Exception {
    fs = startInstance("2.0.4", "3_1", "127.0.0.1", "127.0.0.1", 9999);
    fs2 = startInstance("2.0.4", "3_2", "127.0.0.2", "127.0.0.1", 9998);    
    fs3 = startInstance("2.0.4", "3_3", "127.0.0.3", "127.0.0.1", 9997);
    Thread.sleep(30000);
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
    for (int i = 0; i < 10000; i++) {
      String incr = "UPDATE userstats  " + " SET value = value + 1 "
              + " WHERE username = '"+i+"' and countername= 'friends' ";
      wrap.getClient().execute_cql3_query(ByteBuffer.wrap((incr).getBytes()), Compression.NONE,
              ConsistencyLevel.ALL);
    }
    assertAllThere("127.0.0.1"); 

    System.out.println("added");
    fs4 = startInstance("2.0.4", "3_4", "127.0.0.4", "127.0.0.1", 9996);
    Thread.sleep(40000);
    assertAllThere("127.0.0.4");
  }
  
  @After
  public void after(){
    fs.getManager().destroy();
    fs2.getManager().destroy();
    fs3.getManager().destroy();
    fs4.getManager().destroy();
  }
}
