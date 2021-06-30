/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.test.StepVerifier;

public class ResultsetTest extends BaseConnectionTest {
  private static String vals = "azertyuiopqsdfghjklmwxcvbn";

  @Test
  void multipleResultSet() {
    sharedConn
        .createStatement(
            "create  procedure multiResultSets() BEGIN  SELECT 'a', 'b'; SELECT 'c', 'd', 'e'; END")
        .execute()
        .subscribe();
    final AtomicBoolean first = new AtomicBoolean(true);
    sharedConn
        .createStatement("call multiResultSets()")
        .execute()
        .subscribe(
            res -> {
              if (first.get()) {
                first.set(false);
                res.map(
                        (row, metadata) -> {
                          Assertions.assertEquals(row.get(0), "a");
                          Assertions.assertEquals(row.get(1), "b");
                          Assertions.assertEquals(row.get("a"), "a");
                          Assertions.assertEquals(row.get("b"), "b");
                          assertThrows(
                              IllegalArgumentException.class,
                              () -> row.get("unknown"),
                              "Column name 'unknown' does not exist in column names [a, b]");
                          return "true";
                        })
                    .subscribe();
              } else {
                res.map(
                        (row, metadata) -> {
                          Assertions.assertEquals(row.get(0), "c");
                          Assertions.assertEquals(row.get(1), "d");
                          Assertions.assertEquals(row.get(2), "e");
                          return "true";
                        })
                    .subscribe();
              }
            });
  }

  private String stLen(int len) {
    StringBuilder sb = new StringBuilder(len);
    Random rand = new Random();
    for (int i = 0; i < len; i++) {
      sb.append(vals.charAt(rand.nextInt(26)));
    }
    return sb.toString();
  }

  @Test
  public void returning() {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 5, 1));

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE INSERT_RETURNING (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    MariadbStatement st =
        sharedConn
            .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES (?), (?)")
            .bind(0, "test1")
            .bind(1, "test2")
            .returnGeneratedValues("id", "test");
    Assertions.assertTrue(st.toString().contains("generatedColumns=[id, test]"));
    st.execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
        .as(StepVerifier::create)
        .expectNext("1test1", "2test2")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES (?), (?)")
        .returnGeneratedValues("id")
        .bind(0, "test3")
        .bind(1, "test4")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("3", "4")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES (?), (?)")
        .returnGeneratedValues()
        .bind(0, "a")
        .bind(1, "b")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
        .as(StepVerifier::create)
        .expectNext("5a", "6b")
        .verifyComplete();
  }

  @Test
  public void returningError() {
    assertThrows(
        Exception.class,
        () -> sharedConn.createStatement("CREATE TABLE tt (id int)").returnGeneratedValues("id"),
        "Cannot add RETURNING clause to query");
    assertThrows(
        Exception.class,
        () -> sharedConn.createStatement("CREATE TABLE tt (? int)").returnGeneratedValues("id"),
        "Cannot add RETURNING clause to query");
    assertThrows(
        Exception.class,
        () ->
            sharedConn.createStatement("DELETE * FROM tt RETURNING id").returnGeneratedValues("id"),
        "Statement already includes RETURNING clause");
    assertThrows(
        Exception.class,
        () ->
            sharedConn
                .createStatement("DELETE * FROM tt WHERE id = ? RETURNING id")
                .returnGeneratedValues("id"),
        "Statement already includes RETURNING clause");
  }

  @Test
  void readResultSet() {
    String[] first = new String[] {stLen(10), stLen(300), stLen(60000), stLen(1000)};
    String[] second = new String[] {stLen(10), stLen(300), stLen(60000), stLen(1000)};
    String[] third = new String[] {stLen(10), stLen(300), stLen(60000), stLen(1000)};

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE readResultSet (a TEXT, b TEXT, c LONGTEXT, d TEXT)")
        .execute()
        .subscribe();
    sharedConn
        .createStatement("INSERT INTO readResultSet VALUES (?,?,?,?), (?,?,?,?), (?,?,?,?)")
        .bind(0, first[0])
        .bind(1, first[1])
        .bind(2, first[2])
        .bind(3, first[3])
        .bind(4, second[0])
        .bind(5, second[1])
        .bind(6, second[2])
        .bind(7, second[3])
        .bind(8, third[0])
        .bind(9, third[1])
        .bind(10, third[2])
        .bind(11, third[3])
        .execute()
        .subscribe();

    sharedConn
        .createStatement("SELECT * FROM readResultSet")
        .execute()
        .flatMap(
            res ->
                res.map(
                    (row, metadata) -> {
                      return row.get(3, String.class)
                          + row.get(1, String.class)
                          + row.get(2, String.class)
                          + row.get(0, String.class);
                    }))
        .as(StepVerifier::create)
        .expectNext(
            first[3] + first[1] + first[2] + first[0],
            second[3] + second[1] + second[2] + second[0],
            third[3] + third[1] + third[2] + third[0])
        .verifyComplete();
  }

  @Test
  void getIndexToBig() {
    getIndexToBig(sharedConn);
    getIndexToBig(sharedConnPrepare);
  }

  void getIndexToBig(MariadbConnection connection) {
    connection
        .createStatement("SELECT 1, 2, ?")
        .bind(0, 3)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      return row.get(0, Long.class) + row.get(5, Long.class);
                    }))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable.getMessage().equals("Column index 5 not in range [0-2]"))
        .verify();
  }

  @Test
  void getIndexToLow() {
    getIndexToLow(sharedConn);
    getIndexToLow(sharedConnPrepare);
  }

  void getIndexToLow(MariadbConnection connection) {
    connection
        .createStatement("SELECT 1, 2, ?")
        .bind(0, 3)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      return row.get(0, Long.class) + row.get(-5, Long.class);
                    }))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable.getMessage().equals("Column index -5 must be positive"))
        .verify();
  }

  private String generateLongText(int len) {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    Random random = new Random();
    return random
        .ints(leftLimit, leftLimit + 1) // rightLimit + 1)
        .limit(len)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  @Test
  public void skippingRes() throws SQLException {
    BigInteger maxAllowedPacket =
        sharedConn
            .createStatement("select @@max_allowed_packet")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, BigInteger.class)))
            .blockLast();
    Assumptions.assumeTrue(maxAllowedPacket.intValue() > 35_000_000);
    sharedConn.createStatement("DROP TABLE IF EXISTS prepare3").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE prepare3 (t1 LONGTEXT, t2 LONGTEXT, t3 LONGTEXT, t4 LONGTEXT, t5 varchar(10))")
        .execute()
        .blockLast();
    skippingRes(sharedConn);
    skippingRes(sharedConnPrepare);
  }

  private void skippingRes(MariadbConnection con) {
    con.createStatement("TRUNCATE prepare3").execute().blockLast();
    String longText = generateLongText(20_000_000);
    String mediumText = generateLongText(10_000_000);
    String smallIntText = generateLongText(60_000);

    con.createStatement("INSERT INTO prepare3 values (?,?,?,?,?)")
        .bind(0, longText)
        .bind(1, mediumText)
        .bind(2, smallIntText)
        .bind(3, "expected")
        .bind(4, "small")
        .execute()
        .blockLast();
    con.createStatement("SELECT * FROM prepare3 WHERE 1=?")
        .bind(0, 1)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      assertEquals("small", row.get(4));
                      assertEquals("expected", row.get(3));
                      assertEquals(smallIntText, row.get(2));
                      assertEquals(mediumText, row.get(1));
                      assertEquals(longText, row.get(0));
                      return row.get(3);
                    }))
        .as(StepVerifier::create)
        .expectNext("expected")
        .verifyComplete();
  }
}
