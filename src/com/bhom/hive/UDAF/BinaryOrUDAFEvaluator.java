package com.bhom.hive.UDAF;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class BinaryOrUDAFEvaluator  extends GenericUDAFEvaluator {
	
	PrimitiveObjectInspector inputOI;
	ObjectInspector outputOI;
	PrimitiveObjectInspector longOI;
	
	@Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
            throws HiveException {
    	
        assert (parameters.length == 1);
        super.init(m, parameters);
       
        if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
            inputOI = (PrimitiveObjectInspector) parameters[0];
        } else {
        	longOI = (PrimitiveObjectInspector) parameters[0];
        }

        // init output object inspectors
        outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
                ObjectInspectorOptions.JAVA);
        
        return outputOI;

    }
	
	static class BinaryOrAgg extends GenericUDAFEvaluator.AbstractAggregationBuffer {
        long sum;
        
        void or (long num){
        	sum |= num;
        }
    }
	
	@Override
	public AggregationBuffer getNewAggregationBuffer() throws HiveException {
		BinaryOrAgg result = new BinaryOrAgg();
		reset(result);
		return result;
	}

	@Override
	public void iterate(AggregationBuffer arg0, Object[] arg1) throws HiveException {
		assert(arg1.length == 1);
		
		if (arg1[0] != null) {
			BinaryOrAgg myagg = (BinaryOrAgg) arg0;
			Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(arg1[0]);
			myagg.or(Long.valueOf(p1.toString()));
		}
	}

	@Override
	public void merge(AggregationBuffer agg, Object partial) throws HiveException {
		if (partial != null){
			BinaryOrAgg myagg = (BinaryOrAgg) agg;
			
			Long partialSum = (Long) longOI.getPrimitiveJavaObject(partial);
			
			myagg.or(partialSum);
		}
	}

	@Override
	public void reset(AggregationBuffer arg0) throws HiveException {
		BinaryOrAgg myagg = (BinaryOrAgg) arg0;
		myagg.sum = 0L;
	}

	@Override
	public Object terminate(AggregationBuffer agg) throws HiveException {
		BinaryOrAgg myagg = (BinaryOrAgg) agg;
		return myagg.sum;
	}

	@Override
	public Object terminatePartial(AggregationBuffer agg) throws HiveException {
		BinaryOrAgg myagg = (BinaryOrAgg) agg;
		return myagg.sum;
	}

}
