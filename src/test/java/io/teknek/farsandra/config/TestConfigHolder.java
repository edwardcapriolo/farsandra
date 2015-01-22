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
}
