package com.codeborne.h2lob;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toByteArray;
import static org.junit.Assert.assertEquals;

public class H2Test {
  String url = "jdbc:h2:mem:play;MODE=ORACLE;MV_STORE=false;DB_CLOSE_DELAY=-1";

  @Test
  public void uploadBlob() throws Exception {
    try (Connection connection = DriverManager.getConnection(url, "sa", "")) {
      createTable(connection);
      addFile(connection, 99999);
      connection.commit();
      assertEquals(102400, loadFile(connection, 99999).length);
    }

    try (Connection connection = DriverManager.getConnection(url, "sa", "")) {
      assertEquals(102400, loadFile(connection, 99999).length);
    }
  }

  private void createTable(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate("CREATE TABLE file_storage " +
          "(id INTEGER not NULL, " +
          " file_name VARCHAR(255), " +
          " file_content BLOB, " +
          " PRIMARY KEY ( id ))");
    }
  }

  public void addFile(Connection connection, long id) throws SQLException, IOException {
    String sql = "INSERT INTO file_storage (id,file_name,file_content) VALUES (?,?,?)";

    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
      pstmt.setLong(1, id);
      pstmt.setString(2, "file" + id);
      pstmt.setBlob(3, new ByteArrayInputStream(generateBinaryContent(102400)));
      pstmt.executeUpdate();
    }
  }

  private byte[] loadFile(Connection connection, long id) throws SQLException, IOException {
    String sql = "select file_content from file_storage where id = ?";

    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
      pstmt.setLong(1, id);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (!rs.next()) throw new IllegalArgumentException("File not found: " + id);
        Blob blob = rs.getBlob(1);
        try {
          return toByteArray(blob.getBinaryStream());
        }
        finally {
          blob.free();
        }
      }
    }
  }

  private byte[] generateBinaryContent(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append('a');
    }
    return sb.toString().getBytes(UTF_8);
  }
}
