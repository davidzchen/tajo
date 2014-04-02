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

import org.apache.avro.util.Utf8;
import org.apache.avro.Schema;

/**
 * Utility methods for Avro support.
 */
public class AvroUtil {
  /**
   * Given an Avro string value, which may be a String or an Avro Utf8, convert
   * the value to a String.
   *
   * @param value The value to convert.
   * @return The value converted to a String.
   */
  public static String fromAvroString(Object value) {
    if (value instanceof Utf8) {
      Utf8 utf8 = (Utf8)value;
      return utf8.toString();
    }
    return value.toString();
  }

  /**
   * Given a schema, check to see if it is a union of a null type and a regular
   * schema, and then return the non-null sub-schema. Otherwise, return the
   * given schema.
   *
   * @param schema The schema to check;
   * @return The non-null portion of a union schema, or the given schema.
   */
  public static Schema getNonNull(Schema schema) {
    if (!schema.getType().equals(Schema.Type.UNION)) {
      return schema;
    }
    List<Schema> schemas = schema.getTypes();
    if (schemas.size() != 2) {
      return schema;
    }
    if (schemas.get(0).getType().equals(Schema.Type.NULL)) {
      return schemas.get(1);
    } else if (schemas.get(1).getType().equals(Schema.Type.NULL)) {
      return schemas.get(0);
    } else {
      return schema;
    }
  }
}
