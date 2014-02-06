package io.teknek.farsandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
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
    
    wrap.getClient().execute_cql3_query(
            ByteBuffer.wrap(("CREATE TABLE userstats ( " + " username varchar, "
                    + " countername varchar, " + " value counter, "
                    + " PRIMARY KEY (username, countername) " + " ) with compact storage  ")
                    .getBytes()), Compression.NONE, ConsistencyLevel.ALL);

    String incr = "UPDATE userstats  " + " SET value = value + 1 "
            + " WHERE username = '5' and countername= 'friends' ";
    wrap.getClient().execute_cql3_query(ByteBuffer.wrap((incr).getBytes()), Compression.NONE,
            ConsistencyLevel.ALL);
    
    for (int i = 0; i < 1000; i++) {
      List<Column> changes = new ArrayList<Column>();
      Column c = new Column();
      c.setName("acol".getBytes());
      c.setValue((i+"").getBytes());
      c.setTimestamp(System.nanoTime());
      changes.add( c );
      List<Column> expected = new ArrayList<Column>();
      if (i!=0) {
        Column c2 = new Column();
        c2.setName("acol".getBytes());
        c2.setValue((i-1+"").getBytes());
        expected.add(c2);
      }
      CASResult res = wrap.getClient().cas(ByteBuffer.wrap("a".getBytes()), "widestring", 
              expected, changes, ConsistencyLevel.SERIAL, ConsistencyLevel.ONE);
      Assert.assertEquals(true, res.success);
    }
    CqlResult res = wrap.getClient().execute_cql3_query(ByteBuffer.wrap("select * from widestring where rowkey = 'a'".getBytes()), Compression.NONE, ConsistencyLevel.ONE);
    System.out.println(res);
    String s = new String(res.rows.get(0).getColumns().get(2).getValue());
    
    Assert.assertEquals("999", s);

    System.out.println("added");
  }
  
  @After
  public void after(){
    fs1.getManager().destroy();
    fs2.getManager().destroy();
    fs3.getManager().destroy();
  }

}
