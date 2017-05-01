// Targeted by JavaCPP version 0.9

package org.wso2.siddhi.gpu.jni;

import java.nio.*;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class SiddhiGpu extends org.wso2.siddhi.gpu.jni.presets.SiddhiGpu {
    static { Loader.load(); }

// Parsed from <DataTypes.h>

/*
 * DataTypes.h
 *
 *  Created on: Jan 20, 2015
 *      Author: prabodha
 */

// #ifndef DATATYPES_H_
// #define DATATYPES_H_

// #include <stdint.h>

@Namespace("SiddhiGpu") public static class DataType extends Pointer {
    static { Loader.load(); }
    public DataType() { allocate(); }
    public DataType(int size) { allocateArray(size); }
    public DataType(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public DataType position(int position) {
        return (DataType)super.position(position);
    }

	/** enum SiddhiGpu::DataType::Value */
	public static final int
		Int       = 0,
		Long      = 1,
		Boolean   = 2,
		Float     = 3,
		Double    = 4,
		StringIn  = 5,
		StringExt = 6,
		None      = 7;

	public static native @Cast("const char*") BytePointer GetTypeName(@Cast("SiddhiGpu::DataType::Value") int _eType);
}

// #pragma pack(1)

@Namespace("SiddhiGpu") public static class AttributeMapping extends Pointer {
    static { Loader.load(); }
    public AttributeMapping() { allocate(); }
    public AttributeMapping(int size) { allocateArray(size); }
    public AttributeMapping(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public AttributeMapping position(int position) {
        return (AttributeMapping)super.position(position);
    }

	/** enum SiddhiGpu::AttributeMapping:: */
	public static final int
		STREAM_INDEX = 0,
		ATTRIBUTE_INDEX = 1;

	public native int from(int i); public native AttributeMapping from(int i, int from);
	@MemberGetter public native IntPointer from();
	public native int to(); public native AttributeMapping to(int to);
}

@Namespace("SiddhiGpu") @NoOffset public static class AttributeMappings extends Pointer {
    static { Loader.load(); }
    public AttributeMappings() { }
    public AttributeMappings(Pointer p) { super(p); }

	public AttributeMappings(int _iMappingCount) { allocate(_iMappingCount); }
	private native void allocate(int _iMappingCount);

	public native void AddMapping(int _iPos, int _iFromStream, int _iFromAttribute, int _iToAttribute);

	public native AttributeMappings Clone();

	public native int i_MappingCount(); public native AttributeMappings i_MappingCount(int i_MappingCount);
	public native AttributeMapping p_Mappings(); public native AttributeMappings p_Mappings(AttributeMapping p_Mappings);
}

// #pragma pack()





// #endif /* DATATYPES_H_ */


// Parsed from <Value.h>

/*
 * Value.h
 *
 *  Created on: Jan 20, 2015
 *      Author: prabodha
 */

// #ifndef VALUE_H_
// #define VALUE_H_

// #include <stdint.h>

@Namespace("SiddhiGpu") public static class Values extends Pointer {
    static { Loader.load(); }
    public Values() { allocate(); }
    public Values(int size) { allocateArray(size); }
    public Values(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public Values position(int position) {
        return (Values)super.position(position);
    }

	public native @Cast("bool") boolean b_BoolVal(); public native Values b_BoolVal(boolean b_BoolVal);
	public native int i_IntVal(); public native Values i_IntVal(int i_IntVal);
	public native long l_LongVal(); public native Values l_LongVal(long l_LongVal);
	public native float f_FloatVal(); public native Values f_FloatVal(float f_FloatVal);
	public native double d_DoubleVal(); public native Values d_DoubleVal(double d_DoubleVal);
	public native @Cast("char") byte z_StringVal(int i); public native Values z_StringVal(int i, byte z_StringVal);
	@MemberGetter public native @Cast("char*") BytePointer z_StringVal(); // set this if strlen < 8
	public native @Cast("char*") BytePointer z_ExtString(); public native Values z_ExtString(BytePointer z_ExtString); // set this if strlen > 8
}




// #endif /* VALUE_H_ */


// Parsed from <GpuQueryRuntime.h>

/*
 * GpuQueryRuntime.h
 *
 *  Created on: Jan 18, 2015
 *      Author: prabodha
 */

// #include <stdlib.h>
// #include <stdio.h>
// #include <vector>

// #ifndef GPUQUERYRUNTIME_H_
// #define GPUQUERYRUNTIME_H_

// #include <stdlib.h>
// #include <stdio.h>
// #include <map>
// #include <vector>
// #include <string.h>
// #include <string>

// #include "GpuMetaEvent.h"
// #include "CommonDefs.h"

@Namespace("SiddhiGpu") @NoOffset public static class GpuQueryRuntime extends Pointer {
    static { Loader.load(); }
    public GpuQueryRuntime() { }
    public GpuQueryRuntime(Pointer p) { super(p); }


	public GpuQueryRuntime(@StdString BytePointer _zQueryName, int _iDeviceId, int _iInputEventBufferSize) { allocate(_zQueryName, _iDeviceId, _iInputEventBufferSize); }
	private native void allocate(@StdString BytePointer _zQueryName, int _iDeviceId, int _iInputEventBufferSize);
	public GpuQueryRuntime(@StdString String _zQueryName, int _iDeviceId, int _iInputEventBufferSize) { allocate(_zQueryName, _iDeviceId, _iInputEventBufferSize); }
	private native void allocate(@StdString String _zQueryName, int _iDeviceId, int _iInputEventBufferSize);

	public native void AddStream(@StdString BytePointer _sStramId, GpuMetaEvent _pMetaEvent);
	public native void AddStream(@StdString String _sStramId, GpuMetaEvent _pMetaEvent);
	public native GpuStreamProcessor GetStream(@StdString BytePointer _sStramId);
	public native GpuStreamProcessor GetStream(@StdString String _sStramId);
	public native void AddProcessor(@StdString BytePointer _sStramId, GpuProcessor _pProcessor);
	public native void AddProcessor(@StdString String _sStramId, GpuProcessor _pProcessor);

	public native @Cast("char*") BytePointer GetInputEventBuffer(@StdString BytePointer _sStramId);
	public native @Cast("char*") ByteBuffer GetInputEventBuffer(@StdString String _sStramId);
	public native int GetInputEventBufferSizeInBytes(@StdString BytePointer _sStramId);
	public native int GetInputEventBufferSizeInBytes(@StdString String _sStramId);

	public native @Cast("bool") boolean Configure();
}





// #endif /* GPUQUERYRUNTIME_H_ */


// Parsed from <GpuStreamProcessor.h>

/*
 * GpuStreamProcessor.h
 *
 *  Created on: Jan 20, 2015
 *      Author: prabodha
 */

// #ifndef GPUSTREAMPROCESSOR_H_
// #define GPUSTREAMPROCESSOR_H_

// #include <stdlib.h>
// #include <stdio.h>
// #include <string.h>
// #include <string>
// #include "CommonDefs.h"
@Namespace("SiddhiGpu") @Opaque public static class GpuProcessorContext extends Pointer {
    public GpuProcessorContext() { }
    public GpuProcessorContext(Pointer p) { super(p); }
}

@Namespace("SiddhiGpu") @NoOffset public static class GpuStreamProcessor extends Pointer {
    static { Loader.load(); }
    public GpuStreamProcessor() { }
    public GpuStreamProcessor(Pointer p) { super(p); }

	public GpuStreamProcessor(@StdString BytePointer _sQueryName, @StdString BytePointer _sStreamId, int _iStreamIndex, GpuMetaEvent _pMetaEvent) { allocate(_sQueryName, _sStreamId, _iStreamIndex, _pMetaEvent); }
	private native void allocate(@StdString BytePointer _sQueryName, @StdString BytePointer _sStreamId, int _iStreamIndex, GpuMetaEvent _pMetaEvent);
	public GpuStreamProcessor(@StdString String _sQueryName, @StdString String _sStreamId, int _iStreamIndex, GpuMetaEvent _pMetaEvent) { allocate(_sQueryName, _sStreamId, _iStreamIndex, _pMetaEvent); }
	private native void allocate(@StdString String _sQueryName, @StdString String _sStreamId, int _iStreamIndex, GpuMetaEvent _pMetaEvent);

	public native @Cast("bool") boolean Configure(int _iDeviceId, int _iInputEventBufferSize);
	public native void Initialize(int _iInputEventBufferSize);
	public native void AddProcessor(GpuProcessor _pProcessor);
	public native int Process(int _iNumEvents);

	public native GpuProcessorContext GetProcessorContext();
}




// #endif /* GPUSTREAMPROCESSOR_H_ */


// Parsed from <GpuProcessor.h>

/*
 * GpuProcessor.h
 *
 *  Created on: Jan 19, 2015
 *      Author: prabodha
 */

// #ifndef GPUPROCESSOR_H_
// #define GPUPROCESSOR_H_

// #include <stdlib.h>
// #include <stdio.h>
// #include <list>
// #include "DataTypes.h"
// #include "GpuMetaEvent.h"
// #include "CommonDefs.h"

@Namespace("SiddhiGpu") @NoOffset public static class GpuProcessor extends Pointer {
    static { Loader.load(); }
    public GpuProcessor() { }
    public GpuProcessor(Pointer p) { super(p); }

	/** enum SiddhiGpu::GpuProcessor::Type */
	public static final int
		FILTER = 0,
		LENGTH_SLIDING_WINDOW = 1,
		LENGTH_BATCH_WINDOW = 2,
		TIME_SLIDING_WINDOW = 3,
		TIME_BATCH_WINDOW = 4,
		JOIN = 5,
		SEQUENCE = 6,
		PATTERN = 7;

	public native void Configure(int _iStreamIndex, GpuProcessor _pPrevProcessor, GpuProcessorContext _pContext, @Cast("FILE*") Pointer _fpLog);
	public native void Init(int _iStreamIndex, GpuMetaEvent _pMetaEvent, int _iInputEventBufferSize);
	public native int Process(int _iStreamIndex, int _iNumEvents);
	public native void Print(@Cast("FILE*") Pointer _fp);
	public native GpuProcessor Clone();

	public native int GetResultEventBufferIndex();
	public native @Cast("char*") BytePointer GetResultEventBuffer();
	public native int GetResultEventBufferSize();

	public native @Cast("SiddhiGpu::GpuProcessor::Type") int GetType();
	public native GpuProcessor GetNext();
	public native void SetNext(GpuProcessor _pProc);
	public native void AddToLast(GpuProcessor _pProc);
	public native void SetThreadBlockSize(int _iThreadBlockSize);

	public native void SetOutputStream(GpuMetaEvent _pOutputStreamMeta, AttributeMappings _pMappings);

	public native void SetCurrentOn(@Cast("bool") boolean _on);
	public native void SetExpiredOn(@Cast("bool") boolean _on);

	public native @Cast("bool") boolean GetCurrentOn();
	public native @Cast("bool") boolean GetExpiredOn();
}





// #endif /* GPUPROCESSOR_H_ */


// Parsed from <GpuMetaEvent.h>

/*
 * GpuMetaEvent.h
 *
 *  Created on: Jan 20, 2015
 *      Author: prabodha
 */

// #ifndef GPUMETAEVENT_H_
// #define GPUMETAEVENT_H_

@Namespace("SiddhiGpu") @NoOffset public static class GpuMetaAttribute extends Pointer {
    static { Loader.load(); }
    public GpuMetaAttribute(Pointer p) { super(p); }
    public GpuMetaAttribute(int size) { allocateArray(size); }
    private native void allocateArray(int size);
    @Override public GpuMetaAttribute position(int position) {
        return (GpuMetaAttribute)super.position(position);
    }

	public GpuMetaAttribute() { allocate(); }
	private native void allocate();

	public GpuMetaAttribute(int _iType, int _iLen, int _iPos) { allocate(_iType, _iLen, _iPos); }
	private native void allocate(int _iType, int _iLen, int _iPos);

	public native int i_Type(); public native GpuMetaAttribute i_Type(int i_Type);
	public native int i_Length(); public native GpuMetaAttribute i_Length(int i_Length);
	public native int i_Position(); public native GpuMetaAttribute i_Position(int i_Position);
}

@Namespace("SiddhiGpu") @NoOffset public static class GpuMetaEvent extends Pointer {
    static { Loader.load(); }
    public GpuMetaEvent() { }
    public GpuMetaEvent(Pointer p) { super(p); }

	public GpuMetaEvent(int _iStreamIndex, int _iAttribCount, int _iEventSize) { allocate(_iStreamIndex, _iAttribCount, _iEventSize); }
	private native void allocate(int _iStreamIndex, int _iAttribCount, int _iEventSize);

	public native GpuMetaEvent Clone();

	public native void SetAttribute(int _iIndex, int _iType, int _iLen, int _iPos);

	public native int i_StreamIndex(); public native GpuMetaEvent i_StreamIndex(int i_StreamIndex);
	public native int i_AttributeCount(); public native GpuMetaEvent i_AttributeCount(int i_AttributeCount);
	public native int i_SizeOfEventInBytes(); public native GpuMetaEvent i_SizeOfEventInBytes(int i_SizeOfEventInBytes);
	public native GpuMetaAttribute p_Attributes(); public native GpuMetaEvent p_Attributes(GpuMetaAttribute p_Attributes);
}




// #endif /* GPUMETAEVENT_H_ */


// Parsed from <GpuKernel.h>

/*
 * GpuKernel.h
 *
 *  Created on: Jan 19, 2015
 *      Author: prabodha
 */

// #ifndef GPUKERNEL_H_
// #define GPUKERNEL_H_

// #include <stdlib.h>
// #include <stdio.h>
// #include "CommonDefs.h"

@Namespace("SiddhiGpu") @NoOffset public static class GpuKernel extends Pointer {
    static { Loader.load(); }
    public GpuKernel() { }
    public GpuKernel(Pointer p) { super(p); }


	public native @Cast("bool") boolean Initialize(int _iStreamIndex, GpuMetaEvent _pMetaEvent, int _iInputEventBufferSize);
	public native void Process(int _iStreamIndex, @ByRef IntPointer _iNumEvents);
	public native void Process(int _iStreamIndex, @ByRef IntBuffer _iNumEvents);
	public native void Process(int _iStreamIndex, @ByRef int[] _iNumEvents);

	public native @Cast("char*") BytePointer GetResultEventBuffer();
	public native int GetResultEventBufferSize();

	public native int GetDeviceId();

	public native int GetResultEventBufferIndex();
	public native void SetResultEventBufferIndex(int _iIndex);

	public native int GetInputEventBufferIndex();
	public native void SetInputEventBufferIndex(int _iIndex);

	public native void SetOutputStream(GpuMetaEvent _pOutputStreamMeta, AttributeMappings _pMappings);
}




// #endif /* GPUKERNEL_H_ */


// Parsed from <GpuFilterProcessor.h>

/*
 * GpuFilterProcessor.h
 *
 *  Created on: Jan 20, 2015
 *      Author: prabodha
 */

// #ifndef GPUFILTERPROCESSOR_H_
// #define GPUFILTERPROCESSOR_H_

// #include <stdio.h>
// #include <stdlib.h>
// #include <string>

// #include "GpuProcessor.h"
// #include "DataTypes.h"
// #include "Value.h"
// #define __STDC_FORMAT_MACROS
// #include <inttypes.h>

@Namespace("SiddhiGpu") @Opaque public static class GpuFilterKernelStandalone extends Pointer {
    public GpuFilterKernelStandalone() { }
    public GpuFilterKernelStandalone(Pointer p) { super(p); }
}

// #pragma pack(1)

/** enum SiddhiGpu::ConditionType */
public static final int
	EXECUTOR_NOOP = 0,

	EXECUTOR_AND = 1,
	EXECUTOR_OR = 2,
	EXECUTOR_NOT = 3,
	EXECUTOR_BOOL = 4,

	EXECUTOR_EQ_BOOL_BOOL = 5,
	EXECUTOR_EQ_INT_INT = 6,
	EXECUTOR_EQ_INT_LONG = 7,
	EXECUTOR_EQ_INT_FLOAT = 8,
	EXECUTOR_EQ_INT_DOUBLE = 9,
	EXECUTOR_EQ_LONG_INT = 10,
	EXECUTOR_EQ_LONG_LONG = 11,
	EXECUTOR_EQ_LONG_FLOAT = 12,
	EXECUTOR_EQ_LONG_DOUBLE = 13,
	EXECUTOR_EQ_FLOAT_INT = 14,
	EXECUTOR_EQ_FLOAT_LONG = 15,
	EXECUTOR_EQ_FLOAT_FLOAT = 16,
	EXECUTOR_EQ_FLOAT_DOUBLE = 17,
	EXECUTOR_EQ_DOUBLE_INT = 18,
	EXECUTOR_EQ_DOUBLE_LONG = 19,
	EXECUTOR_EQ_DOUBLE_FLOAT = 20,
	EXECUTOR_EQ_DOUBLE_DOUBLE = 21,
	EXECUTOR_EQ_STRING_STRING = 22,

	EXECUTOR_NE_BOOL_BOOL = 23,
	EXECUTOR_NE_INT_INT = 24,
	EXECUTOR_NE_INT_LONG = 25,
	EXECUTOR_NE_INT_FLOAT = 26,
	EXECUTOR_NE_INT_DOUBLE = 27,
	EXECUTOR_NE_LONG_INT = 28,
	EXECUTOR_NE_LONG_LONG = 29,
	EXECUTOR_NE_LONG_FLOAT = 30,
	EXECUTOR_NE_LONG_DOUBLE = 31,
	EXECUTOR_NE_FLOAT_INT = 32,
	EXECUTOR_NE_FLOAT_LONG = 33,
	EXECUTOR_NE_FLOAT_FLOAT = 34,
	EXECUTOR_NE_FLOAT_DOUBLE = 35,
	EXECUTOR_NE_DOUBLE_INT = 36,
	EXECUTOR_NE_DOUBLE_LONG = 37,
	EXECUTOR_NE_DOUBLE_FLOAT = 38,
	EXECUTOR_NE_DOUBLE_DOUBLE = 39,
	EXECUTOR_NE_STRING_STRING = 40,

	EXECUTOR_GT_INT_INT = 41,
	EXECUTOR_GT_INT_LONG = 42,
	EXECUTOR_GT_INT_FLOAT = 43,
	EXECUTOR_GT_INT_DOUBLE = 44,
	EXECUTOR_GT_LONG_INT = 45,
	EXECUTOR_GT_LONG_LONG = 46,
	EXECUTOR_GT_LONG_FLOAT = 47,
	EXECUTOR_GT_LONG_DOUBLE = 48,
	EXECUTOR_GT_FLOAT_INT = 49,
	EXECUTOR_GT_FLOAT_LONG = 50,
	EXECUTOR_GT_FLOAT_FLOAT = 51,
	EXECUTOR_GT_FLOAT_DOUBLE = 52,
	EXECUTOR_GT_DOUBLE_INT = 53,
	EXECUTOR_GT_DOUBLE_LONG = 54,
	EXECUTOR_GT_DOUBLE_FLOAT = 55,
	EXECUTOR_GT_DOUBLE_DOUBLE = 56,

	EXECUTOR_LT_INT_INT = 57,
	EXECUTOR_LT_INT_LONG = 58,
	EXECUTOR_LT_INT_FLOAT = 59,
	EXECUTOR_LT_INT_DOUBLE = 60,
	EXECUTOR_LT_LONG_INT = 61,
	EXECUTOR_LT_LONG_LONG = 62,
	EXECUTOR_LT_LONG_FLOAT = 63,
	EXECUTOR_LT_LONG_DOUBLE = 64,
	EXECUTOR_LT_FLOAT_INT = 65,
	EXECUTOR_LT_FLOAT_LONG = 66,
	EXECUTOR_LT_FLOAT_FLOAT = 67,
	EXECUTOR_LT_FLOAT_DOUBLE = 68,
	EXECUTOR_LT_DOUBLE_INT = 69,
	EXECUTOR_LT_DOUBLE_LONG = 70,
	EXECUTOR_LT_DOUBLE_FLOAT = 71,
	EXECUTOR_LT_DOUBLE_DOUBLE = 72,

	EXECUTOR_GE_INT_INT = 73,
	EXECUTOR_GE_INT_LONG = 74,
	EXECUTOR_GE_INT_FLOAT = 75,
	EXECUTOR_GE_INT_DOUBLE = 76,
	EXECUTOR_GE_LONG_INT = 77,
	EXECUTOR_GE_LONG_LONG = 78,
	EXECUTOR_GE_LONG_FLOAT = 79,
	EXECUTOR_GE_LONG_DOUBLE = 80,
	EXECUTOR_GE_FLOAT_INT = 81,
	EXECUTOR_GE_FLOAT_LONG = 82,
	EXECUTOR_GE_FLOAT_FLOAT = 83,
	EXECUTOR_GE_FLOAT_DOUBLE = 84,
	EXECUTOR_GE_DOUBLE_INT = 85,
	EXECUTOR_GE_DOUBLE_LONG = 86,
	EXECUTOR_GE_DOUBLE_FLOAT = 87,
	EXECUTOR_GE_DOUBLE_DOUBLE = 88,

	EXECUTOR_LE_INT_INT = 89,
	EXECUTOR_LE_INT_LONG = 90,
	EXECUTOR_LE_INT_FLOAT = 91,
	EXECUTOR_LE_INT_DOUBLE = 92,
	EXECUTOR_LE_LONG_INT = 93,
	EXECUTOR_LE_LONG_LONG = 94,
	EXECUTOR_LE_LONG_FLOAT = 95,
	EXECUTOR_LE_LONG_DOUBLE = 96,
	EXECUTOR_LE_FLOAT_INT = 97,
	EXECUTOR_LE_FLOAT_LONG = 98,
	EXECUTOR_LE_FLOAT_FLOAT = 99,
	EXECUTOR_LE_FLOAT_DOUBLE = 100,
	EXECUTOR_LE_DOUBLE_INT = 101,
	EXECUTOR_LE_DOUBLE_LONG = 102,
	EXECUTOR_LE_DOUBLE_FLOAT = 103,
	EXECUTOR_LE_DOUBLE_DOUBLE = 104,

	EXECUTOR_CONTAINS = 105,

	EXECUTOR_INVALID = 106, // set this for const and var nodes

	EXECUTOR_CONDITION_COUNT = 107;

@Namespace("SiddhiGpu") public static native @Cast("const char*") BytePointer GetConditionName(@Cast("SiddhiGpu::ConditionType") int _eType);

/** enum SiddhiGpu::ExpressionType */
public static final int
	EXPRESSION_CONST = 0,
	EXPRESSION_VARIABLE = 1,
	EXPRESSION_TIME = 2,
	EXPRESSION_SEQUENCE = 3,

	EXPRESSION_ADD_INT = 4,
	EXPRESSION_ADD_LONG = 5,
	EXPRESSION_ADD_FLOAT = 6,
	EXPRESSION_ADD_DOUBLE = 7,

	EXPRESSION_SUB_INT = 8,
	EXPRESSION_SUB_LONG = 9,
	EXPRESSION_SUB_FLOAT = 10,
	EXPRESSION_SUB_DOUBLE = 11,

	EXPRESSION_MUL_INT = 12,
	EXPRESSION_MUL_LONG = 13,
	EXPRESSION_MUL_FLOAT = 14,
	EXPRESSION_MUL_DOUBLE = 15,

	EXPRESSION_DIV_INT = 16,
	EXPRESSION_DIV_LONG = 17,
	EXPRESSION_DIV_FLOAT = 18,
	EXPRESSION_DIV_DOUBLE = 19,

	EXPRESSION_MOD_INT = 20,
	EXPRESSION_MOD_LONG = 21,
	EXPRESSION_MOD_FLOAT = 22,
	EXPRESSION_MOD_DOUBLE = 23,

	EXPRESSION_INVALID = 24,
	EXPRESSION_COUNT = 25;

@Namespace("SiddhiGpu") public static native @Cast("const char*") BytePointer GetExpressionTypeName(@Cast("SiddhiGpu::ExpressionType") int _eType);

/** enum SiddhiGpu::ExecutorNodeType */
public static final int
	EXECUTOR_NODE_CONDITION = 0,
	EXECUTOR_NODE_EXPRESSION = 1,

	EXECUTOR_NODE_TYPE_COUNT = 2;

@Namespace("SiddhiGpu") public static native @Cast("const char*") BytePointer GetNodeTypeName(@Cast("SiddhiGpu::ExecutorNodeType") int _eType);

@Namespace("SiddhiGpu") public static class VariableValue extends Pointer {
    static { Loader.load(); }
    public VariableValue() { allocate(); }
    public VariableValue(int size) { allocateArray(size); }
    public VariableValue(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public VariableValue position(int position) {
        return (VariableValue)super.position(position);
    }

	public native int i_StreamIndex(); public native VariableValue i_StreamIndex(int i_StreamIndex);
	public native @Cast("SiddhiGpu::DataType::Value") int e_Type(); public native VariableValue e_Type(int e_Type);
	public native int i_AttributePosition(); public native VariableValue i_AttributePosition(int i_AttributePosition);

//	VariableValue();
//	VariableValue(int _iStreamIndex, DataType::Value _eType, int _iPos);

	public native @ByRef VariableValue Init();

	public native @ByRef VariableValue SetStreamIndex(int _iStreamIndex);
	public native @ByRef VariableValue SetDataType(@Cast("SiddhiGpu::DataType::Value") int _eType);
	public native @ByRef VariableValue SetPosition(int _iPos);

	public native void Print(@Cast("FILE*") Pointer _fp/*=stdout*/);
	public native void Print();
}

@Namespace("SiddhiGpu") public static class ConstValue extends Pointer {
    static { Loader.load(); }
    public ConstValue() { allocate(); }
    public ConstValue(int size) { allocateArray(size); }
    public ConstValue(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public ConstValue position(int position) {
        return (ConstValue)super.position(position);
    }

	public native @Cast("SiddhiGpu::DataType::Value") int e_Type(); public native ConstValue e_Type(int e_Type);
	public native @ByRef Values m_Value(); public native ConstValue m_Value(Values m_Value);

//	ConstValue();

	public native @ByRef ConstValue Init();

	public native @ByRef ConstValue SetBool(@Cast("bool") boolean _bVal);
	public native @ByRef ConstValue SetInt(int _iVal);
	public native @ByRef ConstValue SetLong(long _lVal);
	public native @ByRef ConstValue SetFloat(float _fval);
	public native @ByRef ConstValue SetDouble(double _dVal);
	public native @ByRef ConstValue SetString(@Cast("const char*") BytePointer _zVal, int _iLen);
	public native @ByRef ConstValue SetString(String _zVal, int _iLen);

	public native void Print(@Cast("FILE*") Pointer _fp/*=stdout*/);
	public native void Print();
}

@Namespace("SiddhiGpu") public static class ElementryNode extends Pointer {
    static { Loader.load(); }
    public ElementryNode() { allocate(); }
    public ElementryNode(int size) { allocateArray(size); }
    public ElementryNode(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public ElementryNode position(int position) {
        return (ElementryNode)super.position(position);
    }

	public native int i_StreamIndex(); public native ElementryNode i_StreamIndex(int i_StreamIndex);
	public native @Cast("SiddhiGpu::DataType::Value") int e_Type(); public native ElementryNode e_Type(int e_Type);
	public native int i_AttributePosition(); public native ElementryNode i_AttributePosition(int i_AttributePosition);
	public native int i_OutputPosition(); public native ElementryNode i_OutputPosition(int i_OutputPosition);
}

@Namespace("SiddhiGpu") public static class ExpressionNode extends Pointer {
    static { Loader.load(); }
    public ExpressionNode() { allocate(); }
    public ExpressionNode(int size) { allocateArray(size); }
    public ExpressionNode(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public ExpressionNode position(int position) {
        return (ExpressionNode)super.position(position);
    }

	/** enum SiddhiGpu::ExpressionNode::Operator */
	public static final int
		OP_ADD_INT = 0,
		OP_ADD_LONG = 1,
		OP_ADD_FLOAT = 2,
		OP_ADD_DOUBLE = 3,

		OP_SUB_INT = 4,
		OP_SUB_LONG = 5,
		OP_SUB_FLOAT = 6,
		OP_SUB_DOUBLE = 7,

		OP_MUL_INT = 8,
		OP_MUL_LONG = 9,
		OP_MUL_FLOAT = 10,
		OP_MUL_DOUBLE = 11,

		OP_DIV_INT = 12,
		OP_DIV_LONG = 13,
		OP_DIV_FLOAT = 14,
		OP_DIV_DOUBLE = 15,

		OP_MOD_INT = 16,
		OP_MOD_LONG = 17,
		OP_MOD_FLOAT = 18,
		OP_MOD_DOUBLE = 19;

	public native @Cast("SiddhiGpu::ExpressionNode::Operator") int e_Operator(); public native ExpressionNode e_Operator(int e_Operator);
	public native int i_LeftValuePos(); public native ExpressionNode i_LeftValuePos(int i_LeftValuePos);
	public native int i_RightValuePos(); public native ExpressionNode i_RightValuePos(int i_RightValuePos);
	public native int i_OutputPosition(); public native ExpressionNode i_OutputPosition(int i_OutputPosition);
}

@Namespace("SiddhiGpu") public static class ConditionNode extends Pointer {
    static { Loader.load(); }
    public ConditionNode() { allocate(); }
    public ConditionNode(int size) { allocateArray(size); }
    public ConditionNode(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public ConditionNode position(int position) {
        return (ConditionNode)super.position(position);
    }

	/** enum SiddhiGpu::ConditionNode::Condition */
	public static final int
		COND_AND = 0,
		COND_OR = 1,
		COND_NOT = 2,
		COND_BOOL = 3,

		COND_EQ_BOOL_BOOL = 4,
		COND_EQ_INT_INT = 5,
		COND_EQ_INT_LONG = 6,
		COND_EQ_INT_FLOAT = 7,
		COND_EQ_INT_DOUBLE = 8,
		COND_EQ_LONG_INT = 9,
		COND_EQ_LONG_LONG = 10,
		COND_EQ_LONG_FLOAT = 11,
		COND_EQ_LONG_DOUBLE = 12,
		COND_EQ_FLOAT_INT = 13,
		COND_EQ_FLOAT_LONG = 14,
		COND_EQ_FLOAT_FLOAT = 15,
		COND_EQ_FLOAT_DOUBLE = 16,
		COND_EQ_DOUBLE_INT = 17,
		COND_EQ_DOUBLE_LONG = 18,
		COND_EQ_DOUBLE_FLOAT = 19,
		COND_EQ_DOUBLE_DOUBLE = 20,
		COND_EQ_STRING_STRING = 21,

		COND_NE_BOOL_BOOL = 22,
		COND_NE_INT_INT = 23,
		COND_NE_INT_LONG = 24,
		COND_NE_INT_FLOAT = 25,
		COND_NE_INT_DOUBLE = 26,
		COND_NE_LONG_INT = 27,
		COND_NE_LONG_LONG = 28,
		COND_NE_LONG_FLOAT = 29,
		COND_NE_LONG_DOUBLE = 30,
		COND_NE_FLOAT_INT = 31,
		COND_NE_FLOAT_LONG = 32,
		COND_NE_FLOAT_FLOAT = 33,
		COND_NE_FLOAT_DOUBLE = 34,
		COND_NE_DOUBLE_INT = 35,
		COND_NE_DOUBLE_LONG = 36,
		COND_NE_DOUBLE_FLOAT = 37,
		COND_NE_DOUBLE_DOUBLE = 38,
		COND_NE_STRING_STRING = 39,

		COND_GT_INT_INT = 40,
		COND_GT_INT_LONG = 41,
		COND_GT_INT_FLOAT = 42,
		COND_GT_INT_DOUBLE = 43,
		COND_GT_LONG_INT = 44,
		COND_GT_LONG_LONG = 45,
		COND_GT_LONG_FLOAT = 46,
		COND_GT_LONG_DOUBLE = 47,
		COND_GT_FLOAT_INT = 48,
		COND_GT_FLOAT_LONG = 49,
		COND_GT_FLOAT_FLOAT = 50,
		COND_GT_FLOAT_DOUBLE = 51,
		COND_GT_DOUBLE_INT = 52,
		COND_GT_DOUBLE_LONG = 53,
		COND_GT_DOUBLE_FLOAT = 54,
		COND_GT_DOUBLE_DOUBLE = 55,

		COND_LT_INT_INT = 56,
		COND_LT_INT_LONG = 57,
		COND_LT_INT_FLOAT = 58,
		COND_LT_INT_DOUBLE = 59,
		COND_LT_LONG_INT = 60,
		COND_LT_LONG_LONG = 61,
		COND_LT_LONG_FLOAT = 62,
		COND_LT_LONG_DOUBLE = 63,
		COND_LT_FLOAT_INT = 64,
		COND_LT_FLOAT_LONG = 65,
		COND_LT_FLOAT_FLOAT = 66,
		COND_LT_FLOAT_DOUBLE = 67,
		COND_LT_DOUBLE_INT = 68,
		COND_LT_DOUBLE_LONG = 69,
		COND_LT_DOUBLE_FLOAT = 70,
		COND_LT_DOUBLE_DOUBLE = 71,

		COND_GE_INT_INT = 72,
		COND_GE_INT_LONG = 73,
		COND_GE_INT_FLOAT = 74,
		COND_GE_INT_DOUBLE = 75,
		COND_GE_LONG_INT = 76,
		COND_GE_LONG_LONG = 77,
		COND_GE_LONG_FLOAT = 78,
		COND_GE_LONG_DOUBLE = 79,
		COND_GE_FLOAT_INT = 80,
		COND_GE_FLOAT_LONG = 81,
		COND_GE_FLOAT_FLOAT = 82,
		COND_GE_FLOAT_DOUBLE = 83,
		COND_GE_DOUBLE_INT = 84,
		COND_GE_DOUBLE_LONG = 85,
		COND_GE_DOUBLE_FLOAT = 86,
		COND_GE_DOUBLE_DOUBLE = 87,

		COND_LE_INT_INT = 88,
		COND_LE_INT_LONG = 89,
		COND_LE_INT_FLOAT = 90,
		COND_LE_INT_DOUBLE = 91,
		COND_LE_LONG_INT = 92,
		COND_LE_LONG_LONG = 93,
		COND_LE_LONG_FLOAT = 94,
		COND_LE_LONG_DOUBLE = 95,
		COND_LE_FLOAT_INT = 96,
		COND_LE_FLOAT_LONG = 97,
		COND_LE_FLOAT_FLOAT = 98,
		COND_LE_FLOAT_DOUBLE = 99,
		COND_LE_DOUBLE_INT = 100,
		COND_LE_DOUBLE_LONG = 101,
		COND_LE_DOUBLE_FLOAT = 102,
		COND_LE_DOUBLE_DOUBLE = 103,

		COND_CONTAINS = 104;

	public native @Cast("SiddhiGpu::ConditionNode::Condition") int e_Condition(); public native ConditionNode e_Condition(int e_Condition);
	public native int i_LeftValuePos(); public native ConditionNode i_LeftValuePos(int i_LeftValuePos);
	public native int i_RightValuePos(); public native ConditionNode i_RightValuePos(int i_RightValuePos);
	public native int i_OutputPosition(); public native ConditionNode i_OutputPosition(int i_OutputPosition);
}

@Namespace("SiddhiGpu") public static class ValueNode extends Pointer {
    static { Loader.load(); }
    public ValueNode() { allocate(); }
    public ValueNode(int size) { allocateArray(size); }
    public ValueNode(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public ValueNode position(int position) {
        return (ValueNode)super.position(position);
    }

	public native @Cast("SiddhiGpu::DataType::Value") int e_Type(); public native ValueNode e_Type(int e_Type);
	public native @ByRef Values m_Value(); public native ValueNode m_Value(Values m_Value);
}

@Namespace("SiddhiGpu") public static class ExecutorNode extends Pointer {
    static { Loader.load(); }
    public ExecutorNode() { allocate(); }
    public ExecutorNode(int size) { allocateArray(size); }
    public ExecutorNode(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(int size);
    @Override public ExecutorNode position(int position) {
        return (ExecutorNode)super.position(position);
    }

	public native @Cast("SiddhiGpu::ExecutorNodeType") int e_NodeType(); public native ExecutorNode e_NodeType(int e_NodeType);

	// if operator - what is the type
	public native @Cast("SiddhiGpu::ConditionType") int e_ConditionType(); public native ExecutorNode e_ConditionType(int e_ConditionType);

	// if expression
	public native @Cast("SiddhiGpu::ExpressionType") int e_ExpressionType(); public native ExecutorNode e_ExpressionType(int e_ExpressionType);

	// if const - what is the value
	public native @ByRef ConstValue m_ConstValue(); public native ExecutorNode m_ConstValue(ConstValue m_ConstValue);

	// if var - variable holder
	public native @ByRef VariableValue m_VarValue(); public native ExecutorNode m_VarValue(VariableValue m_VarValue);

	public native int i_ParentNodeIndex(); public native ExecutorNode i_ParentNodeIndex(int i_ParentNodeIndex);
	public native @Cast("bool") boolean b_Processed(); public native ExecutorNode b_Processed(boolean b_Processed);

//	ExecutorNode();
	public native @ByRef ExecutorNode Init();

	public native @ByRef ExecutorNode SetNodeType(@Cast("SiddhiGpu::ExecutorNodeType") int _eNodeType);
	public native @ByRef ExecutorNode SetConditionType(@Cast("SiddhiGpu::ConditionType") int _eCondType);
	public native @ByRef ExecutorNode SetExpressionType(@Cast("SiddhiGpu::ExpressionType") int _eExprType);
	public native @ByRef ExecutorNode SetConstValue(@ByVal ConstValue _mConstVal);
	public native @ByRef ExecutorNode SetVariableValue(@ByVal VariableValue _mVarValue);

	public native @ByRef ExecutorNode SetParentNode(int _iParentIndex);

	public native void Print();
	public native void Print(@Cast("FILE*") Pointer _fp);
}

// #pragma pack()

@Namespace("SiddhiGpu") @NoOffset public static class GpuFilterProcessor extends GpuProcessor {
    static { Loader.load(); }
    public GpuFilterProcessor() { }
    public GpuFilterProcessor(Pointer p) { super(p); }

	public GpuFilterProcessor(int _iNodeCount) { allocate(_iNodeCount); }
	private native void allocate(int _iNodeCount);

	public native void AddExecutorNode(int _iPos, @ByRef ExecutorNode _pNode);

	public native void Destroy();

	public native void Configure(int _iStreamIndex, GpuProcessor _pPrevProcessor, GpuProcessorContext _pContext, @Cast("FILE*") Pointer _fpLog);
	public native void Init(int _iStreamIndex, GpuMetaEvent _pMetaEvent, int _iInputEventBufferSize);
	public native int Process(int _iStreamIndex, int _iNumEvents);
	public native void Print(@Cast("FILE*") Pointer _fp);
	public native GpuProcessor Clone();

	public native int GetResultEventBufferIndex();
	public native @Cast("char*") BytePointer GetResultEventBuffer();
	public native int GetResultEventBufferSize();

	public native void Print();

	public native ExecutorNode ap_ExecutorNodes(); public native GpuFilterProcessor ap_ExecutorNodes(ExecutorNode ap_ExecutorNodes); // nodes are stored in in-order
	public native int i_NodeCount(); public native GpuFilterProcessor i_NodeCount(int i_NodeCount);
}




// #endif /* GPUFILTERPROCESSOR_H_ */


// Parsed from <GpuLengthSlidingWindowProcessor.h>

/*
 * GpuLengthSlidingWindowProcessor.h
 *
 *  Created on: Jan 20, 2015
 *      Author: prabodha
 */

// #ifndef GPULENGTHSLIDINGWINDOWPROCESSOR_H_
// #define GPULENGTHSLIDINGWINDOWPROCESSOR_H_

// #include <stdio.h>
// #include "GpuProcessor.h"
@Namespace("SiddhiGpu") @Opaque public static class GpuLengthSlidingWindowFilterKernel extends Pointer {
    public GpuLengthSlidingWindowFilterKernel() { }
    public GpuLengthSlidingWindowFilterKernel(Pointer p) { super(p); }
}

@Namespace("SiddhiGpu") @NoOffset public static class GpuLengthSlidingWindowProcessor extends GpuProcessor {
    static { Loader.load(); }
    public GpuLengthSlidingWindowProcessor() { }
    public GpuLengthSlidingWindowProcessor(Pointer p) { super(p); }

	public GpuLengthSlidingWindowProcessor(int _iWindowSize) { allocate(_iWindowSize); }
	private native void allocate(int _iWindowSize);

	public native void Configure(int _iStreamIndex, GpuProcessor _pPrevProcessor, GpuProcessorContext _pContext, @Cast("FILE*") Pointer _fpLog);
	public native void Init(int _iStreamIndex, GpuMetaEvent _pMetaEvent, int _iInputEventBufferSize);
	public native int Process(int _iStreamIndex, int _iNumEvents);
	public native void Print(@Cast("FILE*") Pointer _fp);
	public native GpuProcessor Clone();
	public native int GetResultEventBufferIndex();
	public native @Cast("char*") BytePointer GetResultEventBuffer();
	public native int GetResultEventBufferSize();

	public native void Print();

	public native int GetWindowSize();
}




// #endif /* GPULENGTHSLIDINGWINDOWPROCESSOR_H_ */


// Parsed from <GpuJoinProcessor.h>

/*
 * GpuJoinProcessor.h
 *
 *  Created on: Jan 31, 2015
 *      Author: prabodha
 */

// #ifndef GPUJOINPROCESSOR_H_
// #define GPUJOINPROCESSOR_H_

// #include <stdio.h>
// #include "GpuProcessor.h"
@Namespace("SiddhiGpu") @Opaque public static class GpuJoinKernel extends Pointer {
    public GpuJoinKernel() { }
    public GpuJoinKernel(Pointer p) { super(p); }
}

@Namespace("SiddhiGpu") @NoOffset public static class GpuJoinProcessor extends GpuProcessor {
    static { Loader.load(); }
    public GpuJoinProcessor() { }
    public GpuJoinProcessor(Pointer p) { super(p); }

	public GpuJoinProcessor(int _iLeftWindowSize, int _iRightWindowSize) { allocate(_iLeftWindowSize, _iRightWindowSize); }
	private native void allocate(int _iLeftWindowSize, int _iRightWindowSize);

	public native void Configure(int _iStreamIndex, GpuProcessor _pPrevProcessor, GpuProcessorContext _pContext, @Cast("FILE*") Pointer _fpLog);
	public native void Init(int _iStreamIndex, GpuMetaEvent _pMetaEvent, int _iInputEventBufferSize);
	public native int Process(int _iStreamIndex, int _iNumEvents);
	public native void Print(@Cast("FILE*") Pointer _fp);
	public native GpuProcessor Clone();
	public native int GetResultEventBufferIndex();
	public native @Cast("char*") BytePointer GetResultEventBuffer();
	public native int GetResultEventBufferSize();

	public native @Cast("char*") BytePointer GetLeftResultEventBuffer();
	public native int GetLeftResultEventBufferSize();
	public native @Cast("char*") BytePointer GetRightResultEventBuffer();
	public native int GetRightResultEventBufferSize();

	public native void Print();

	public native void SetLeftStreamWindowSize(int _iWindowSize);
	public native void SetRightStreamWindowSize(int _iWindowSize);
	public native int GetLeftStreamWindowSize();
	public native int GetRightStreamWindowSize();

	public native void SetLeftTrigger(@Cast("bool") boolean _bTrigger);
	public native void SetRightTrigger(@Cast("bool") boolean _bTrigger);
	public native @Cast("bool") boolean GetLeftTrigger();
	public native @Cast("bool") boolean GetRightTrigger();

	public native void SetWithInTimeMilliSeconds(@Cast("uint64_t") long _iTime);
	public native @Cast("uint64_t") long GetWithInTimeMilliSeconds();

	public native void SetExecutorNodes(int _iNodeCount);
	public native void AddExecutorNode(int _iPos, @ByRef ExecutorNode _pNode);

	public native void SetThreadWorkSize(int _iSize);
	public native int GetThreadWorkSize();

	public native int i_NodeCount(); public native GpuJoinProcessor i_NodeCount(int i_NodeCount);
	public native ExecutorNode ap_ExecutorNodes(); public native GpuJoinProcessor ap_ExecutorNodes(ExecutorNode ap_ExecutorNodes);
}




// #endif /* GPUJOINPROCESSOR_H_ */


}
