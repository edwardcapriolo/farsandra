package io.teknek.farsandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.After;
import org.junit.Test;

public class CompareAndSwapTest {
  
  Farsandra fs1;
  Farsandra fs2;    
  Farsandra fs3;
  
  @Test
  public void threeNodeTest() throws Exception {
    fs1 = UnflushedJoinTest.startInstance("2.0.4", "3_1", "127.0.0.1", "127.0.0.1", 9999);
    fs2 = UnflushedJoinTest.startInstance("2.0.4", "3_2", "127.0.0.2", "127.0.0.1", 9998);    
    fs3 = UnflushedJoinTest.startInstance("2.0.4", "3_3", "127.0.0.3", "127.0.0.1", 9997);
    
    FramedConnWrapper wrap = new FramedConnWrapper("127.0.0.1", 9160);
    String ks = "CREATE KEYSPACE test "
            + " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 2} ";
    wrap.open();
    wrap.getClient().execute_cql3_query(ByteBuffer.wrap((ks).getBytes()), Compression.NONE,
            ConsistencyLevel.ALL);
    wrap.getClient().set_keyspace("test");
    wrap.getClient().execute_cql3_query(
            ByteBuffer.wrap(("CREATE TABLE widestring ( " + " rowkey varchar, "
                    + " name varchar, " + " value varchar, "
                    + " PRIMARY KEY (rowkey, name) " + " ) with compact storage  ")
                    .getBytes()), Compression.NONE, ConsistencyLevel.ALL);

    Thread.sleep(10000);
    for (int i = 0; i < 10000; i++) {
      /* String incr = "UPDATE userstats  " + " SET value = value + 1 "
              + " WHERE username = '"+i+"' and countername= 'friends' ";
      wrap.getClient().execute_cql3_query(ByteBuffer.wrap((incr).getBytes()), Compression.NONE,
              ConsistencyLevel.ALL); */
      List<Column> changes = new ArrayList<Column>();
      Column c = new Column();
      c.setName("acol".getBytes());
      c.setValue("a_value".getBytes());
      c.setTimestamp(System.nanoTime());
      changes.add( c );
      wrap.getClient().cas(ByteBuffer.wrap("a".getBytes()), "widestring", 
              new ArrayList<Column>(), changes, ConsistencyLevel.SERIAL, ConsistencyLevel.ONE);
      System.out.println("worked once");
    }
     

    System.out.println("added");
  }
  
  @After
  public void after(){
    fs1.getManager().destroy();
    fs2.getManager().destroy();
    fs3.getManager().destroy();
  }

}
