package com.lambdai.thriftdao.bson;

import com.foursquare.common.thrift.bson.BSONReadState;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;

public class LBSONReadState extends BSONReadState {
    protected TField nextField() throws TException {
        Object item = currentReadContext().getNextItem();
        if (item == null) {
            // We've exhausted all the fields in the current document.
            return NO_MORE_FIELDS;
        }
        try {
            // The next field name is a BSON document key, which must be a string.
            String name = (String)item;
            // We parse the name to get the id
            return new TField(name, currentReadContext().valueTType(), Short.parseShort(name));
        } catch (ClassCastException e) {
            throw new TException("Expected string document key but got a " + item.getClass().getName());
        }
    }
}
