/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tsfile.utils;

import org.apache.tsfile.common.constant.JsonFormatConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RecordUtils is a utility class for parsing data in form of CSV string. */
public class RecordUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RecordUtils.class);

  /**
   * support input format: {@code <deviceId>,<timestamp>,[<measurementId>,<value>,]}.CSV line is
   * separated by ","
   *
   * @param str - input string
   * @param schema - constructed file schema
   * @return TSRecord constructed from str
   */
  public static TSRecord parseSimpleTupleRecord(String str, Schema schema) {
    // split items
    String[] items = str.split(JsonFormatConstant.TSRECORD_SEPARATOR);
    // get deviceId and timestamp, then create a new TSRecord
    String deviceId = items[0].trim();
    long timestamp;
    try {
      timestamp = Long.parseLong(items[1].trim());
    } catch (NumberFormatException e) {
      LOG.warn("given timestamp is illegal:{}", str);
      // return a TSRecord without any data points
      return new TSRecord(deviceId, -1);
    }
    TSRecord ret = new TSRecord(deviceId, timestamp);

    // loop all rest items except the last one
    String measurementId;
    TSDataType type;
    for (int i = 2; i < items.length - 1; i += 2) {
      // get measurementId and value
      measurementId = items[i].trim();
      MeasurementGroup measurementGroup =
          schema.getSeriesSchema(Factory.DEFAULT_FACTORY.create(deviceId));
      IMeasurementSchema measurementSchema =
          measurementGroup == null
              ? null
              : measurementGroup.getMeasurementSchemaMap().get(measurementId);
      if (measurementSchema == null) {
        LOG.warn("measurementId:{},type not found, pass", measurementId);
        continue;
      }
      type = measurementSchema.getType();
      String value = items[i + 1].trim();
      // if value is not null, wrap it with corresponding DataPoint and add to
      // TSRecord
      if (!"".equals(value)) {
        try {
          switch (type) {
            case INT32:
            case DATE:
              ret.addTuple(new IntDataPoint(measurementId, Integer.parseInt(value)));
              break;
            case INT64:
            case TIMESTAMP:
              ret.addTuple(new LongDataPoint(measurementId, Long.parseLong(value)));
              break;
            case FLOAT:
              ret.addTuple(new FloatDataPoint(measurementId, Float.parseFloat(value)));
              break;
            case DOUBLE:
              ret.addTuple(new DoubleDataPoint(measurementId, Double.parseDouble(value)));
              break;
            case BOOLEAN:
              ret.addTuple(new BooleanDataPoint(measurementId, Boolean.parseBoolean(value)));
              break;
            case TEXT:
            case BLOB:
            case STRING:
              ret.addTuple(new StringDataPoint(measurementId, BytesUtils.valueOf(items[i + 1])));
              break;
            default:
              LOG.warn("unsupported data type:{}", type);
              break;
          }
        } catch (NumberFormatException e) {
          LOG.warn("parsing measurement meets error, omit it", e);
        }
      }
    }
    return ret;
  }
}
