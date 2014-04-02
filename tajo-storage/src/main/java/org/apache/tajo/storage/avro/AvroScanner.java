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

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;

import java.io.IOException;

/**
 * FileScanner for reading Avro files
 */
public class AvroScanner extends FileScanner {
  Schema avroSchema;
  DataFileReader<GenericRecord> dataFileReader;

  /**
   * Creates a new AvroScanner.
   *
   * @param conf
   * @param schema
   * @param meta
   * @param fragment
   */
  public AvroScanner(Configuration conf,
                     final org.apache.tajo.catalog.Schema schema,
                     final TableMeta meta, final FileFragment fragment) {
    super(conf, schema, meta, fragment);
  }

  /**
   * Initializes the AvroScanner.
   */
  @Override
  public void init() throws IOException {
    if (targets == null) {
      targets = schema.toArray();
    }

    String schemaString = tableMeta.getOption(AVRO_SCHEMA);
    if (schemaString == null) {
      throw new RuntimeException("No Avro schema for table.");
    }
    avroSchema = new org.apache.avro.Schema.Parser().parse(schemaString);

    DatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>(avroSchema);
    dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
    super.init();
  }

  /**
   * Reads the next Tuple from the Avro file.
   *
   * @return The next Tuple from the Avro file or null if end of file is
   *         reached.
   */
  @Override
  public Tuple next() throws IOException {
    if (!dataFileReader.hasNext()) {
      return null;
    }

    Tuple tuple = new VTuple(schema.size());
    GenericRecord record = dataFileReader.next();
    List<Schema.Field> avroFields = avroSchema.getFields();
    int index = 0;
    for (int avroIndex = 0; avroIndex < avroFields.size(); ++avroIndex) {
      Schema.Field avroField = avroFields.get(avroIndex);
      Schema nonNullAvroSchema = AvroUtil.getNonNull(avroField.schema());
      Schema.Type avroType = nonNullAvroSchema.getType();
      Object value = record.get(avroIndex);
      switch (avroType) {
        case BOOLEAN:
          tuple.put(index, DatumFactory.createBool((Boolean)value));
          break;
        case INT:
          tuple.put(index, DatumFactory.createInt4((Integer)value));
          break;
        case LONG:
          tuple.put(index, DatumFactory.createInt8((Long)value));
          break;
        case FLOAT:
          tuple.put(index, DatumFactory.createFloat4((Float)value));
          break;
        case DOUBLE:
          tuple.put(index, DatumFactory.createFloat8((Double)value));
          break;
        case BYTES:
          tuple.put(index, new BlobDatum((ByteBuffer)value));
          break;
        case STRING:
          tuple.put(index,
                    DatumFactory.createText(AvroUtil.fromAvroString(value));
          break;
        case RECORD:
          throw new RuntimeException("Avro RECORD not supported.");
        case ENUM:
          throw new RuntimeException("Avro ENUM not supported.");
        case MAP:
          throw new RuntimeException("Avro MAP not supported.");
        case UNION:
          throw new RuntimeException("Avro UNION not supported.");
        case FIXED:
          tuple.put(index, new BlobDatum(((GenericFixed)value).bytes()));
          break;
        default:
          throw new RuntimeException("Unknown type.");
      }
      ++index;
    }
    return tuple;
  }

  /**
   * Resets the scanner
   */
  @Override
  public void reset() throws IOException {
  }

  /**
   * Closes the scanner.
   */
  @Override
  public void close() throws IOException {
    dataFileReader.close();
  }

  /**
   * Returns whether this scanner is projectable.
   *
   * @return true
   */
  @Override
  public boolean isProjectable() {
    return true;
  }

  /**
   * Returns whether this scanner is selectable.
   *
   * @return false
   */
  @Override
  public boolean isSelectable() {
    return false;
  }

  /**
   * Returns whether this scanner is splittable.
   *
   * @return false
   */
  @Override
  public boolean isSplittable() {
    return false;
  }
}
