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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.TableMeta;

public class AvroUtil {
  public static Schema getAvroSchema(TableMeta meta, Configuration conf)
      throws IOException {
    String schemaLiteral = meta.getOption(CatalogConstants.AVRO_SCHEMA_LITERAL);
    String schemaUrl = meta.getOption(CatalogConstants.AVRO_SCHEMA_URL);
    if (schemaLiteral == null && schemaUrl == null) {
      throw new RuntimeException("No Avro schema for table.");
    }
    if (schemaLiteral != null) {
      return new Schema.Parser().parse(schemaLiteral);
    }
    Path schemaPath = new Path(schemaUrl);
    FileSystem fs = schemaPath.getFileSystem(conf);
    FSDataInputStream inputStream = fs.open(schemaPath);
    return new Schema.Parser().parse(inputStream);
  }
}
