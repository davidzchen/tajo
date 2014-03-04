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
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.catalog.Schema;

import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.api.ReadSupport;

/**
 * Read Tajo records from a Parquet file
 */
public class TajoParquetReader extends ParquetReader<Tuple> {
  public TajoParquetReader(Path file, Schema requestedSchema) throws IOException {
    super(file, new TajoReadSupport(requestedSchema));
  }

  public TajoParquetReader(Path file, Schema requestedSchema,
                           UnboundRecordFilter recordFilter)
      throws IOException {
    super(file, new TajoReadSupport(requestedSchema), recordFilter);
  }
}
