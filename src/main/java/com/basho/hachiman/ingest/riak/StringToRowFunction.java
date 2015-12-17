package com.basho.hachiman.ingest.riak;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.Row;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rx.functions.Func1;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.springframework.util.StringUtils.commaDelimitedListToStringArray;

/**
 * A function that maps an incoming string message to a TS Row of typed Cells.
 */
@Component
public class StringToRowFunction implements Func1<String, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(StringToRowFunction.class);

  private final ObjectMapper mapper;

  private String[] schema;

  @Autowired
  public StringToRowFunction(ObjectMapper mapper,
                             PipelineConfig pipelineConfig) {
    this.mapper = mapper;
    this.schema = commaDelimitedListToStringArray(pipelineConfig.getRiak().getSchema());
  }

  @Override
  public Row call(String msg) {
    // Require all values to be strings in the JSON. We'll convert them based on the schema.
    List<String> row = null;
    try {
      row = mapper.readValue(msg, new TypeReference<List<String>>() {
      });
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read row {} from message string.", row);
    }

    FastList<Cell> cells = FastList.newList(schema.length);
    if (schema.length != row.size()) {
      throw new IllegalStateException("Row does not conform to schema. Expected "
                                      + Arrays.toString(schema)
                                      + " but got "
                                      + row);
    }
    for (int len = row.size(), i = 0; i < len; i++) {
      switch (schema[i]) {
        case "set":
          throw new IllegalArgumentException("DDL type 'set' not supported");
        case "timestamp":
          cells.add(Cell.newTimestamp(Long.parseLong(row.get(i))));
          continue;
        case "double":
          cells.add(new Cell(Double.parseDouble(row.get(i))));
          continue;
        case "sint64":
          cells.add(new Cell(Long.parseLong(row.get(i))));
          continue;
        case "boolean":
          cells.add(new Cell(Boolean.parseBoolean(row.get(i))));
          continue;
        default:
          cells.add(new Cell(row.get(i)));
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Converted to Riak TS row: {}", row);
    }

    return new Row(cells);
  }

}
