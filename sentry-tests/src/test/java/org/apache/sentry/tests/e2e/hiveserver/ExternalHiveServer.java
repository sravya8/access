/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.hiveserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;


public class ExternalHiveServer implements HiveServer{
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ExternalHiveServer.class);
  private final File confDir;
  private final File logDir;
  private Process process;

  public static final String auth = System.getProperty("auth", "kerberos");
  public static final String hs2Host = System.getProperty("hs2Host", "hive-secure-2.ent.cloudera.com");
  public static final String hivePrinc = System.getProperty("hivePrinc", "hive/_HOST@ENT.CLOUDERA.COM");

  public ExternalHiveServer(HiveConf hiveConf, File confDir, File logDir) throws Exception {
    this.confDir = confDir;
    this.logDir = logDir;

    classSetup();
  }

  public static void classSetup(){
    String driverName = "org.apache.hive.jdbc.HiveDriver";

    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
  }

  @Override
  public synchronized void start() throws Exception {
  }

  @Override
  public synchronized void shutdown() throws Exception {
  }

  @Override
  public String getURL() {
    return "jdbc:hive2://" + hs2Host + ":10000/default;";
  }

  @Override
  public String getProperty(String key) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Connection createConnection(String user, String password) throws Exception{
    String commandFormat = "kinit -kt /cdep/keytabs/%s.keytab %s@ENT.CLOUDERA.COM";
    String command = String.format(commandFormat, user, user);
    Process proc = Runtime.getRuntime().exec(command);
    String url = getURL();
    Properties oProps = new Properties();

    if(auth.equals("kerberos")){
      url += "principal=" + hivePrinc;
      oProps.setProperty("IMPERSONATE", user);
    }else{
      oProps.setProperty("user",user);
      oProps.setProperty("password",password);
    }
    return DriverManager.getConnection(url, oProps);
  }

  public void kdestroy() throws Exception{

    String command = "kdestroy";
    Process proc = Runtime.getRuntime().exec(command);

  }
}
