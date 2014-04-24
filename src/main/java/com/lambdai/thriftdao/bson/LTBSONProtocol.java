package com.lambdai.thriftdao.bson;

import com.foursquare.common.thrift.base.EnhancedTField;
import com.foursquare.common.thrift.bson.TBSONObjectProtocol;
import com.foursquare.common.thrift.bson.TBSONProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;

public class LTBSONProtocol extends TBSONProtocol {
    public static class WriterFactory extends TBSONObjectProtocol.WriterFactoryForVanillaBSONObject
            implements TProtocolFactory {
        public TProtocol getProtocol(TTransport trans) {
            return super.doGetProtocol(trans);
        }

        protected TBSONObjectProtocol createBlankProtocol(TTransport trans) { return doCreateBlankProtocol(trans); }
    }

    public static class ReaderFactory extends TBSONObjectProtocol.ReaderFactory implements TProtocolFactory {
        public TBSONObjectProtocol getProtocol(TTransport trans) {
            return doGetProtocol(trans);
        }
        protected TBSONObjectProtocol doGetProtocol(TTransport trans) {
            // Set up the protocol for reading.
            TBSONObjectProtocol ret = createBlankProtocol(trans);
            ret.setReadState(new LBSONReadState());
            return ret;
        }
        protected TBSONObjectProtocol createBlankProtocol(TTransport trans) { return doCreateBlankProtocol(trans); }
    }

    // need this for hive. It uses reflection and expects a #Factory static class that implements TProtocolFactory
    public static class Factory extends ReaderFactory {}

    protected static TBSONObjectProtocol doCreateBlankProtocol(TTransport trans) {
        LTBSONProtocol ret = new LTBSONProtocol();
        ret.realTransport = trans;
        return ret;
    }

    @Override
    public void writeFieldBegin(TField tField) throws TException {
        // Write Id instead of name
        writeState.putValue(Short.toString(tField.id));
        if (tField instanceof EnhancedTField) {
            enhancedType = ((EnhancedTField)tField).enhancedTypes.get("bson");
        }
    }
}
