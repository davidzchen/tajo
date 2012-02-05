package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;

/**
 * @author Hyunsik Choi
 */
public final class SumInt extends Function {
  public SumInt() {
    super(new Column[] { new Column("arg1", DataType.INT)});
  }

  @Override
  public Datum invoke(final Datum... data) {
    if(data.length == 1) {
      return data[0];
    }
    
    return data[0].plus(data[1]);
  }

  @Override
  public DataType getResType() {
    return DataType.INT;
  }
}
