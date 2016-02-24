package io.teknek.farsandra.config;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestConfigHolder {

  @Test
  public void configHolderTest() throws InterruptedException{
    ConfigHolder config = new ConfigHolder();
    assertNotNull(config);
    assertEquals(config.getProperties().getProperty("cassandra.package.name.prefix"),"apache-cassandra-");
    assertEquals(config.getProperties().getProperty("cassandra.package.name.suffix"),"-bin.tar.gz");
  }
  
  @Test
  public void configHolderCustomPropertiesTest() throws InterruptedException{
    ConfigHolder config = new ConfigHolder("farsandra_custom.properties");
    assertNotNull(config);
    assertEquals(config.getProperties().getProperty("cassandra.package.name.prefix"),"apache-cassandra-");
    assertEquals(config.getProperties().getProperty("cassandra.package.name.suffix"),"-bin.tar.gz");
    assertEquals(config.getProperties().getProperty("farsandra.home.folder"),".farsandra");
  }
}
