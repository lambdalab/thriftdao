package com.lambdai.thriftdao.bson;

import com.foursquare.common.thrift.bson.TBSONObjectProtocol;
import com.foursquare.common.thrift.bson.TBSONProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.Field;

public class LTBSONProtocol extends TBSONProtocol {
  public static class WriterFactory extends TBSONProtocol.WriterFactory {
    protected TBSONObjectProtocol createBlankProtocol(TTransport trans) { return doCreateBlankProtocol(trans); }
  }
  public static class ReaderFactory extends TBSONProtocol.ReaderFactory {
    protected TBSONObjectProtocol createBlankProtocol(TTransport trans) { return doCreateBlankProtocol(trans); }
  }
  public static class Factory extends ReaderFactory {}

  static TBSONObjectProtocol doCreateBlankProtocol(TTransport trans) {
      LTBSONProtocol ret = new LTBSONProtocol();
      ret.realTransport = trans;
      return ret;
  }

  @Override
  public TField readFieldBegin() throws TException {
    TField res = super.readFieldBegin();
    if (res.type == TType.STOP) {
      return res;
    }
    return new TField(res.name, res.type, Short.parseShort(res.name));
  }

  @Override
  public void writeFieldBegin(TField tField) throws TException {
    super.writeFieldBegin(new TField(Short.toString(tField.id), tField.type, tField.id));
  }
}
