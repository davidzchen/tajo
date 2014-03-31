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

package org.apache.tajo.storage.parquet;

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

/**
 * FileAppender for writing to Parquet files.
 */
public class ParquetAppender extends FileAppender {
  private TajoParquetWriter writer;
  private int blockSize;
  private int pageSize;
  private CompressionCodecName compressionCodecName;
  private boolean enableDictionary;
  private boolean validating;
  private TableStatistics stats;

  /**
   * Creates a new ParquetAppender.
   *
   * @param conf Configuration properties.
   * @param schema The table schema.
   * @param meta The table metadata.
   * @param path The path of the Parquet file to write to.
   */
  public ParquetAppender(Configuration conf, Schema schema, TableMeta meta,
                         Path path) throws IOException {
    super(conf, schema, meta, path);
    String value = meta.getOption(ParquetOutputFormat.BLOCK_SIZE);
    if (value == null) {
      this.blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
    } else {
      this.blockSize = Integer.parseInt(value);
    }

    value = meta.getOption(ParquetOutputFormat.PAGE_SIZE);
    if (value == null) {
      this.pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
    } else {
      this.pageSize = Integer.parseInt(value);
    }

    value = meta.getOption(ParquetOutputFormat.COMPRESSION);
    if (value == null) {
      // When parquet-hadoop 1.3.3 is available, this should be changed to
      // ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME.
      this.compressionCodecName = CompressionCodecName.UNCOMPRESSED;
    } else {
      this.compressionCodecName = CompressionCodecName.fromConf(value);
    }

    value = meta.getOption(ParquetOutputFormat.ENABLE_DICTIONARY);
    if (value == null) {
      // When parquet-hadoop 1.3.3 is available, this should be changed to
      // ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED.
      this.enableDictionary = true;
    } else {
      this.enableDictionary = Boolean.parseBoolean(value);
    }

    value = meta.getOption(ParquetOutputFormat.VALIDATION);
    if (value == null) {
      // When parquet-hadoop 1.3.3 is available, this should be changed to
      // ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED.
      this.validating = false;
    } else {
      this.validating = Boolean.parseBoolean(value);
    }
  }

  /**
   * Initializes the Appender. This method creates a new TajoParquetWriter
   * and initializes the table statistics if enabled.
   */
  public void init() throws IOException {
    writer = new TajoParquetWriter(path,
                                   schema,
                                   compressionCodecName,
                                   blockSize,
                                   pageSize,
                                   enableDictionary,
                                   validating);
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
   * Write a Tuple to the Parquet file.
   *
   * @param tuple The Tuple to write.
   */
  @Override
  public void addTuple(Tuple tuple) throws IOException {
    if (enabledStats) {
      for (int i = 0; i < schema.size(); ++i) {
        stats.analyzeField(i, tuple.get(i));
      }
    }
    writer.write(tuple);
    if (enabledStats) {
      stats.incrementRow();
    }
  }

  /**
   * The ParquetWriter does not need to be flushed, so this is a no-op.
   */
  @Override
  public void flush() throws IOException {
  }

  /**
   * Closes the Appender.
   */
  @Override
  public void close() throws IOException {
    writer.close();
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
