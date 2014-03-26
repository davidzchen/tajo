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

import java.util.Map;

import parquet.Log;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.storage.Tuple;

/**
 * Tajo implementation of {@link ReadSupport} for {@link Tuple}s.
 * Users should use {@link ParquetScanner} and not this class directly.
 */
public class TajoReadSupport extends ReadSupport<Tuple> {
  private static final Log LOG = Log.getLog(TajoReadSupport.class);

  private Schema readSchema;
  private Schema requestedSchema;

  /**
   * The key for the Tajo schema stored in the Parquet file metadata.
   */
  public static final String TAJO_SCHEMA_METADATA_KEY = "tajo.schema";

  /**
   * Creates a new TajoReadSupport.
   *
   * @param requestedSchema The Tajo schema of the requested projection passed
   *        down by ParquetScanner.
   */
  public TajoReadSupport(Schema readSchema, Schema requestedSchema) {
    super();
    this.readSchema = readSchema;
    this.requestedSchema = requestedSchema;
  }

  /**
   * Creates a new TajoReadSupport.
   *
   * @param requestedSchema The Tajo schema of the requested projection passed
   *        down by ParquetScanner.
   */
  public TajoReadSupport(Schema readSchema) {
    super();
    this.readSchema = readSchema;
    this.requestedSchema = readSchema;
  }

  /**
   * Initializes the ReadSupport.
   *
   * @param context The InitContext.
   * @return A ReadContext that defines how to read the file.
   */
  @Override
  public ReadContext init(InitContext context) {
    if (requestedSchema == null) {
      throw new RuntimeException("requestedSchema is null.");
    }
    MessageType requestedParquetSchema =
      new TajoSchemaConverter().convert(requestedSchema);
    LOG.debug("Reading data with projection:\n" + requestedParquetSchema);
    return new ReadContext(requestedParquetSchema);
  }

  /**
   * Prepares for read.
   *
   * @param configuration The job configuration.
   * @param keyValueMetaData App-specific metadata from the file.
   * @param fileSchema The schema of the Parquet file.
   * @param readContext Returned by the init method.
   */
  @Override
  public RecordMaterializer<Tuple> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    Schema tajoReadSchema = null;
    String metadataReadSchema = keyValueMetaData.get(TAJO_SCHEMA_METADATA_KEY);
    if (metadataReadSchema != null) {
      tajoReadSchema = CatalogGsonHelper.fromJson(
          metadataReadSchema, Schema.class);
    } else {
      tajoReadSchema = readSchema;
    }
    MessageType parquetRequestedSchema = readContext.getRequestedSchema();
    return new TajoRecordMaterializer(parquetRequestedSchema, requestedSchema,
                                      tajoReadSchema);
  }
}
