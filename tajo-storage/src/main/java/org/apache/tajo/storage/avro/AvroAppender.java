/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

/**
 * FileAppender for writing to Avro files.
 */
public class AvroAppender extends FileAppender {
  private FileSystem fs;
  private TableStatistics stats;
  private Schema avroSchema;

  /**
   * Creates a new AvroAppender.
   *
   * @param conf Configuration properties.
   * @param schema The table schema.
   * @param meta The table metadata.
   * @param path The path of the Parquet file to write to.
   */
  public AvroAppender(Configuration conf, org.apache.avro.Schema schema,
                      TableMeta meta, Path path) throws IOException {
    super(conf, schema, meta, path);
  }

  /**
   * Initializes the Appender.
   */
  public void init() throws IOException {
    fs = path.getFileSystem(conf);
    if (!fs.exists(path.getParent()) {
      throw new FileNotFoundException(path.toString());
    }

    String schemaString = tableMeta.getOption(AVRO_SCHEMA);
    if (schemaString == null) {
      throw new RuntimeException("No Avro schema for table.");
    }
    avroSchema = new org.apache.avro.Schema.Parser().parse(schemaString);

    if (enabledStats) {
      this.stats = new TableStatistics(schema);
    }
    super.init();
  }

  /**
   * Gets the current offset. Tracking offsets is currenly not implemented, so
   * this method always returns 0.
   *
   * @return 0
   */
  @Override
  public long getOffset() throws IOException {
    return 0;
  }

  /**
   * Write a Tuple to the Avro file.
   *
   * @param tuple The Tuple to write.
   */
  @Override
  public void addTuple(Tuple tuple) throws IOException {
    for (int i = 0; i < schema.size(); ++i) {
      Column column = schema.getColumn(i);
      if (enabledStats) {
        stats.analyzeField(i, tuple.get(i));
      }
      switch (type) {
        case NULL_TYPE:
          break;
        case BOOLEAN:
          break;
        case BIT:
        case INT2:
        case INT4:
          break;
        case INT8:
          break;
        case FLOAT4:
          break;
        case FLOAT8:
          break;
        case CHAR:
        case TEXT:
          break;
        case PROTOBUF:
        case BLOB:
        case INET4:
        case INET6:
          break;
        default:
          throw new RuntimeException("Cannot convert to Avro type: " + type);
      }
    }

    if (enabledStats) {
      stats.incrementRow();
    }
  }

  /**
   */
  @Override
  public void flush() throws IOException {
  }

  /**
   * Closes the Appender.
   */
  @Override
  public void close() throws IOException {
  }

  /**
   * If table statistics is enabled, retrieve the table statistics.
   *
   * @return Table statistics if enabled or null otherwise.
   */
  @Override
  public TableStats getStats() {
    if (enabledStats) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
