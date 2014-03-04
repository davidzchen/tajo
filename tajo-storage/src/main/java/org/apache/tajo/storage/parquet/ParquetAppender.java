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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;

public class ParquetAppender extends FileAppender {
  private TajoParquetWriter writer;

  public ParquetAppender(Configuration conf, Schema schema, TableMeta meta,
                         Path path) throws IOException {
    super(conf, schema, meta, path);
  }

  public void init() throws IOException {
    writer = new TajoParquetWriter(path, schema);
  }

  @Override
  public long getOffset() throws IOException {
    return 0;
  }

  @Override
  public void addTuple(Tuple tuple) throws IOException {
    writer.write(tuple);
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public TableStats getStats() {
    return null;
  }
}
