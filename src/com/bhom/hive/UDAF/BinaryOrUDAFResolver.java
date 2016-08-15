/**
 * 
 */
package com.bhom.hive.UDAF;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;


/**
 * Takes a column of integer type values, or a column of lists of integer types,
 *  and outputs the binary OR aggregate of all elts in that column.
 */
class BinaryOrUDAFResolver extends AbstractGenericUDAFResolver {
	static final Log LOG = LogFactory.getLog(BinaryOrUDAFResolver.class.getName());
	
	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
		ObjectInspector [] parameters = info.getParameterObjectInspectors();
	    if (parameters.length != 1) {
	      throw new UDFArgumentTypeException(parameters.length - 1,
	          "Please specify exactly one argument.");
	    }
	     
	    // validate the first parameter, which is the expression to compute over
	    if (parameters[0].getCategory() == ObjectInspector.Category.PRIMITIVE) {
	    	switch (((PrimitiveObjectInspector) parameters[0]).getPrimitiveCategory()){
    		case INT:
    		case LONG:
    		case SHORT:
    			return new BinaryOrUDAFEvaluator();
			default:
				break;
	    	}
	    	
	    }
	    else if (parameters[0].getCategory() == ObjectInspector.Category.LIST) {
	    	ObjectInspector listElementOI = ((ListObjectInspector) parameters[0]).getListElementObjectInspector();
	    	if (listElementOI.getCategory() ==  ObjectInspector.Category.PRIMITIVE){
	    		switch (((PrimitiveObjectInspector) listElementOI).getPrimitiveCategory()){
	    		case INT:
	    		case LONG:
	    		case SHORT:
	    			return new ListBinaryOrUDAFEvaluator();
				default:
					break;
		    	}
	    	}
	    }
    	throw new UDFArgumentTypeException(0,
  	          "Only integers and lists of integers are accepted but "
  	          + parameters[0].getTypeName() + " was passed as parameter 1.");
	}
}
