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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.Tuple;

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;

/**
 * Write Tajo records to a Parquet file.
 */
public class TajoParquetWriter extends ParquetWriter<Tuple> {
  /**
   * Create a new {@link TajoParquetWriter}
   *
   * @param file
   * @param schema
   * @param compressionCodecName
   * @param blockSize
   * @param pageSize
   * @throws IOException
   */
  public TajoParquetWriter(Path file,
                           Schema schema,
                           CompressionCodecName compressionCodecName,
                           int blockSize,
                           int pageSize) throws IOException {
    super(file,
          new TajoWriteSupport(schema),
          compressionCodecName,
          blockSize,
          pageSize);
  }

  /**
   * Create a new {@link TajoParquetWriter}
   *
   * @param file The file name to write to
   * @param schema
   * @param compressionCodec Compression codec to use, or CompressionCodecName.UNCOMPRESSED.
   * @param blockSize the block size threshold.
   * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment.
   * @param enableDictionary Whether to use a dictionary to compress columns.
   * @param validating to turn on validation
   * @throws IOException
   */
  public TajoParquetWriter(Path file,
                           Schema schema,
                           CompressionCodecName compressionCodecName,
                           int blockSize,
                           int pageSize,
                           boolean enableDictionary,
                           boolean validating) throws IOException {
    super(file,
          new TajoWriteSupport(schema),
          compressionCodecName,
          blockSize,
          pageSize,
          enableDictionary,
          validating);
  }

  /**
   * Creates a new {@link TajoParquetWriter}. The default block size is 50 MB.
   * The default page size is 1 MB. Default compression is no compression.
   *
   * @param file
   * @param schema
   * @throws IOException
   */
  public TajoParquetWriter(Path file, Schema schema) throws IOException {
    this(file,
         schema,
         CompressionCodecName.UNCOMPRESSED,
         DEFAULT_BLOCK_SIZE,
         DEFAULT_PAGE_SIZE);
  }
}
