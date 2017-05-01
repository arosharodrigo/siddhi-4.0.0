package org.wso2.siddhi.core.gpu.util.parser;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.gpu.config.GpuQueryContext;
import org.wso2.siddhi.gpu.jni.SiddhiGpu;
import org.wso2.siddhi.gpu.jni.SiddhiGpu.ConstValue;
import org.wso2.siddhi.gpu.jni.SiddhiGpu.ExecutorNode;
import org.wso2.siddhi.gpu.jni.SiddhiGpu.GpuFilterProcessor;
import org.wso2.siddhi.gpu.jni.SiddhiGpu.VariableValue;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.condition.And;
import org.wso2.siddhi.query.api.expression.condition.Compare;
import org.wso2.siddhi.query.api.expression.condition.Not;
import org.wso2.siddhi.query.api.expression.condition.Or;
import org.wso2.siddhi.query.api.expression.constant.*;
import org.wso2.siddhi.query.api.expression.math.*;

import java.util.ArrayList;
import java.util.List;

import static org.wso2.siddhi.core.util.SiddhiConstants.*;

public class GpuExpressionParser
{
    private static final Logger log = Logger.getLogger(GpuExpressionParser.class);
    
    public SiddhiGpu.GpuFilterProcessor parseFilterExpression(Expression expression, MetaComplexEvent metaEvent, int currentState,
                                                              ExecutionPlanContext executionPlanContext, GpuQueryContext gpuQueryContext) {

        log.info("<" + gpuQueryContext.getQueryName() + "> parseFilterExpression");
        log.info("<" + gpuQueryContext.getQueryName() + "> Root Expression = " + expression.toString());

        List<SiddhiGpu.ExecutorNode> gpuFilterList = new ArrayList<SiddhiGpu.ExecutorNode>();

        parseExpressionTree(expression, metaEvent, currentState, executionPlanContext, gpuFilterList, -1);

        SiddhiGpu.GpuFilterProcessor filter = new SiddhiGpu.GpuFilterProcessor(gpuFilterList.size());

        int i = 0;
        for (SiddhiGpu.ExecutorNode executorNode : gpuFilterList) {
            filter.AddExecutorNode(i, executorNode);
            i++;
        }

        return filter;
    }
    
    public void parseJoinOnCompareExpression(Expression expression, MetaComplexEvent metaEvent, int currentState,
                                             ExecutionPlanContext executionPlanContext, GpuQueryContext gpuQueryContext, SiddhiGpu.GpuJoinProcessor gpuJoinProcessor) {

        log.info("<" + gpuQueryContext.getQueryName() + "> parseJoinOnCompareExpression");
        log.info("<" + gpuQueryContext.getQueryName() + "> Root Expression = " + expression.toString());

        List<SiddhiGpu.ExecutorNode> gpuFilterList = new ArrayList<SiddhiGpu.ExecutorNode>();

        parseExpressionTree(expression, metaEvent, currentState, executionPlanContext, gpuFilterList, -1);

        gpuJoinProcessor.SetExecutorNodes(gpuFilterList.size());

        int i = 0;
        for (SiddhiGpu.ExecutorNode executorNode : gpuFilterList) {
            gpuJoinProcessor.AddExecutorNode(i, executorNode);
            i++;
        }

    }

    public Attribute.Type parseExpressionTree(Expression expression, MetaComplexEvent metaEvent, int currentState,
                                              ExecutionPlanContext executionPlanContext,
                                              List<SiddhiGpu.ExecutorNode> gpuFilterList,
                                              int parentNodeIndex) {
        
        int currentNodeIndex = gpuFilterList.size();
        
        log.debug("Expression = " + expression.toString());

        if (expression instanceof And) {
            gpuFilterList.add(new SiddhiGpu.ExecutorNode().Init()
                    .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                    .SetConditionType(SiddhiGpu.EXECUTOR_AND)
                    .SetParentNode(parentNodeIndex));
            
            parseExpressionTree(((And) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            parseExpressionTree(((And) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            return Attribute.Type.BOOL;
        }
        else if (expression instanceof Or) {
            gpuFilterList.add(new SiddhiGpu.ExecutorNode().Init()
                    .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                    .SetConditionType(SiddhiGpu.EXECUTOR_OR)
                    .SetParentNode(parentNodeIndex));
            
            parseExpressionTree(((Or) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            parseExpressionTree(((Or) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            return Attribute.Type.BOOL;
        }
        else if (expression instanceof Not) {
            gpuFilterList.add(new SiddhiGpu.ExecutorNode().Init()
                    .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                    .SetConditionType(SiddhiGpu.EXECUTOR_NOT)
                    .SetParentNode(parentNodeIndex));
            
            parseExpressionTree(((Not) expression).getExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            return Attribute.Type.BOOL;
        }
        else if (expression instanceof Compare) {
            if (((Compare) expression).getOperator() == Compare.Operator.EQUAL)
            {
                SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                        .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                        .SetParentNode(parentNodeIndex);
                gpuFilterList.add(node);
                Attribute.Type lhs = parseExpressionTree(((Compare) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                Attribute.Type rhs = parseExpressionTree(((Compare) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                parseEqualCompare(lhs, rhs, node);	
                return Attribute.Type.BOOL;
            }
            else if (((Compare) expression).getOperator() == Compare.Operator.NOT_EQUAL)
            {
                SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init().
                        SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                        .SetParentNode(parentNodeIndex);
                gpuFilterList.add(node);
                Attribute.Type lhs = parseExpressionTree(((Compare) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                Attribute.Type rhs = parseExpressionTree(((Compare) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                parseNotEqualCompare(lhs, rhs, node);
                return Attribute.Type.BOOL;
            }
            else if (((Compare) expression).getOperator() == Compare.Operator.GREATER_THAN)
            {
                SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                        .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                        .SetParentNode(parentNodeIndex);
                gpuFilterList.add(node);
                Attribute.Type lhs = parseExpressionTree(((Compare) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                Attribute.Type rhs = parseExpressionTree(((Compare) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                parseGreaterThanCompare(lhs, rhs, node);
                return Attribute.Type.BOOL;
            }
            else if (((Compare) expression).getOperator() == Compare.Operator.GREATER_THAN_EQUAL)
            {
                SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                        .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                        .SetParentNode(parentNodeIndex);
                gpuFilterList.add(node);
                Attribute.Type lhs = parseExpressionTree(((Compare) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                Attribute.Type rhs = parseExpressionTree(((Compare) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                parseGreaterThanEqualCompare(lhs, rhs, node);
                return Attribute.Type.BOOL;
            }
            else if (((Compare) expression).getOperator() == Compare.Operator.LESS_THAN)
            {
                SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                        .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                        .SetParentNode(parentNodeIndex);
                gpuFilterList.add(node);
                Attribute.Type lhs = parseExpressionTree(((Compare) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                Attribute.Type rhs = parseExpressionTree(((Compare) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                parseLessThanCompare(lhs, rhs, node);
                return Attribute.Type.BOOL;
            }
            else if (((Compare) expression).getOperator() == Compare.Operator.LESS_THAN_EQUAL)
            {
                SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init().
                        SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                        .SetParentNode(parentNodeIndex);
                gpuFilterList.add(node);
                Attribute.Type lhs = parseExpressionTree(((Compare) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                Attribute.Type rhs = parseExpressionTree(((Compare) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                parseLessThanEqualCompare(lhs, rhs, node);
                return Attribute.Type.BOOL;
            }
            else if (((Compare) expression).getOperator() == Compare.Operator.CONTAINS)
            {
                SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                        .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                        .SetParentNode(parentNodeIndex);
                gpuFilterList.add(node);
                Attribute.Type lhs = parseExpressionTree(((Compare) expression).getLeftExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                Attribute.Type rhs = parseExpressionTree(((Compare) expression).getRightExpression(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
                parseContainsCompare(lhs, rhs, node);
                return Attribute.Type.BOOL;
            }

        }
        else if (expression instanceof Constant)
        {
            if (expression instanceof BoolConstant)
            {
                gpuFilterList.add(new ExecutorNode().Init()
                .SetNodeType(SiddhiGpu.EXECUTOR_NODE_EXPRESSION)
                .SetExpressionType(SiddhiGpu.EXPRESSION_CONST)
                .SetParentNode(parentNodeIndex)
                .SetConstValue(new ConstValue().Init().SetBool(((BoolConstant) expression).getValue())));
                return Attribute.Type.BOOL;
            }
            else if (expression instanceof StringConstant)
            {
                String strVal = ((StringConstant) expression).getValue();
                gpuFilterList.add(new ExecutorNode().Init()
                .SetNodeType(SiddhiGpu.EXECUTOR_NODE_EXPRESSION)
                .SetExpressionType(SiddhiGpu.EXPRESSION_CONST)
                .SetParentNode(parentNodeIndex)
                .SetConstValue(new ConstValue().Init().SetString(strVal, strVal.length())));
                return Attribute.Type.STRING;
            }
            else if (expression instanceof IntConstant)
            {
                gpuFilterList.add(new ExecutorNode().Init()
                .SetNodeType(SiddhiGpu.EXECUTOR_NODE_EXPRESSION)
                .SetExpressionType(SiddhiGpu.EXPRESSION_CONST)
                .SetParentNode(parentNodeIndex)
                .SetConstValue(new ConstValue().Init().SetInt(((IntConstant) expression).getValue())));
                return Attribute.Type.INT;
            }
            else if (expression instanceof LongConstant)
            {
                gpuFilterList.add(new ExecutorNode().Init()
                .SetNodeType(SiddhiGpu.EXECUTOR_NODE_EXPRESSION)
                .SetExpressionType(SiddhiGpu.EXPRESSION_CONST)
                .SetParentNode(parentNodeIndex)
                .SetConstValue(new ConstValue().Init().SetLong(((LongConstant) expression).getValue())));
                return Attribute.Type.LONG;
            }
            else if (expression instanceof FloatConstant)
            {
                gpuFilterList.add(new ExecutorNode().Init()
                .SetNodeType(SiddhiGpu.EXECUTOR_NODE_EXPRESSION)
                .SetExpressionType(SiddhiGpu.EXPRESSION_CONST)
                .SetParentNode(parentNodeIndex)
                .SetConstValue(new ConstValue().Init().SetFloat(((FloatConstant) expression).getValue())));
                return Attribute.Type.FLOAT;
            }
            else if (expression instanceof DoubleConstant)
            {
                gpuFilterList.add(new ExecutorNode().Init()
                .SetNodeType(SiddhiGpu.EXECUTOR_NODE_EXPRESSION)
                .SetExpressionType(SiddhiGpu.EXPRESSION_CONST)
                .SetParentNode(parentNodeIndex)
                .SetConstValue(new ConstValue().Init().SetDouble(((DoubleConstant) expression).getValue())));
                return Attribute.Type.DOUBLE;
            }

        }
        else if (expression instanceof Variable)
        {
            return parseVariable((Variable) expression, metaEvent, currentState, gpuFilterList, currentNodeIndex);
        }
        else if (expression instanceof Multiply)
        {
            SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                    .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                    .SetParentNode(parentNodeIndex);
            gpuFilterList.add(node);
            Attribute.Type lhs = parseExpressionTree(((Multiply) expression).getLeftValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type rhs = parseExpressionTree(((Multiply) expression).getRightValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type type = parseArithmeticOperationResultType(lhs, rhs);

            switch (type)
            {
            case INT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_MUL_INT);
                break;
            case LONG:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_MUL_LONG);
                break;
            case FLOAT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_MUL_FLOAT);
                break;
            case DOUBLE:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_MUL_DOUBLE);
                break;
            default: 
                break;
            }

            return type;
        }
        else if (expression instanceof Add)
        {
            SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                    .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                    .SetParentNode(parentNodeIndex);
            gpuFilterList.add(node);
            Attribute.Type lhs = parseExpressionTree(((Add) expression).getLeftValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type rhs = parseExpressionTree(((Add) expression).getRightValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type type = parseArithmeticOperationResultType(lhs, rhs);

            switch (type)
            {
            case INT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_ADD_INT);
                break;
            case LONG:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_ADD_LONG);
                break;
            case FLOAT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_ADD_FLOAT);
                break;
            case DOUBLE:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_ADD_DOUBLE);
                break;
            default: 
                break;
            }

            return type;

        }
        else if (expression instanceof Subtract)
        {
            SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                    .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                    .SetParentNode(parentNodeIndex);
            gpuFilterList.add(node);
            Attribute.Type lhs = parseExpressionTree(((Subtract) expression).getLeftValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type rhs = parseExpressionTree(((Subtract) expression).getRightValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type type = parseArithmeticOperationResultType(lhs, rhs);

            switch (type)
            {
            case INT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_SUB_INT);
                break;
            case LONG:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_SUB_LONG);
                break;
            case FLOAT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_SUB_FLOAT);
                break;
            case DOUBLE:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_SUB_DOUBLE);
                break;
            default: 
                break;
            }

            return type;
        }
        else if (expression instanceof Mod)
        {
            SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                    .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                    .SetParentNode(parentNodeIndex);
            gpuFilterList.add(node);
            Attribute.Type lhs = parseExpressionTree(((Mod) expression).getLeftValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type rhs = parseExpressionTree(((Mod) expression).getRightValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type type = parseArithmeticOperationResultType(lhs, rhs);

            switch (type)
            {
            case INT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_MOD_INT);
                break;
            case LONG:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_MOD_LONG);
                break;
            case FLOAT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_MOD_FLOAT);
                break;
            case DOUBLE:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_MOD_DOUBLE);
                break;
            default: 
                break;
            }

            return type;
        }
        else if (expression instanceof Divide)
        {
            SiddhiGpu.ExecutorNode node = new SiddhiGpu.ExecutorNode().Init()
                    .SetNodeType(SiddhiGpu.EXECUTOR_NODE_CONDITION)
                    .SetParentNode(parentNodeIndex);
            gpuFilterList.add(node);
            Attribute.Type lhs = parseExpressionTree(((Divide) expression).getLeftValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type rhs = parseExpressionTree(((Divide) expression).getRightValue(), metaEvent, currentState, executionPlanContext, gpuFilterList, currentNodeIndex);
            Attribute.Type type = parseArithmeticOperationResultType(lhs, rhs);

            switch (type)
            {
            case INT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_DIV_INT);
                break;
            case LONG:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_DIV_LONG);
                break;
            case FLOAT:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_DIV_FLOAT);
                break;
            case DOUBLE:
                node.SetExpressionType(SiddhiGpu.EXPRESSION_DIV_DOUBLE);
                break;
            default: 
                break;
            }

            return type;

        }

        throw new UnsupportedOperationException(expression.toString() + " not supported!");

    }

    private void parseGreaterThanCompare(Attribute.Type lhsType, Attribute.Type rhsType, SiddhiGpu.ExecutorNode node)
    {
        switch (lhsType)
        {
        case STRING:
        {
            throw new OperationNotSupportedException("string cannot used in greater than comparisons");
        }
        case INT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("int cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_INT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_INT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_INT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_INT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("int cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("int cannot be compared with " + rhsType);
            }
            break;
        }
        case LONG:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("long cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_LONG_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_LONG_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_LONG_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_LONG_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("long cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("long cannot be compared with " + rhsType);
            }
            break;
        }
        case FLOAT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("float cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_FLOAT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_FLOAT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_FLOAT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_FLOAT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("float cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("float cannot be compared with " + rhsType);
            }
            break;
        }
        case DOUBLE:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("double cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_DOUBLE_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_DOUBLE_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_DOUBLE_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GT_DOUBLE_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("double cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("double cannot be compared with " + rhsType);
            }
            break;
        }
        case BOOL:
        {
            throw new OperationNotSupportedException("bool cannot used in greater than comparisons");
        }
        default:
        {
            throw new OperationNotSupportedException(lhsType + " cannot be used in greater than comparisons");
        }
        }
    }

    private void parseGreaterThanEqualCompare(Attribute.Type lhsType, Attribute.Type rhsType, SiddhiGpu.ExecutorNode node)
    {
        switch (lhsType)
        {
        case STRING:
        {
            throw new OperationNotSupportedException("string cannot used in greater than equal comparisons");
        }
        case INT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("int cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_INT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_INT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_INT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_INT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("int cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("int cannot be compared with " + rhsType);
            }
            break;
        }
        case LONG:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("long cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_LONG_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_LONG_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_LONG_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_LONG_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("long cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("long cannot be compared with " + rhsType);
            }
            break;
        }
        case FLOAT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("float cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_FLOAT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_FLOAT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_FLOAT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_FLOAT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("float cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("float cannot be compared with " + rhsType);
            }
            break;
        }
        case DOUBLE:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("double cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_DOUBLE_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_DOUBLE_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_DOUBLE_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_GE_DOUBLE_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("double cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("double cannot be compared with " + rhsType);
            }
            break;
        }
        case BOOL:
        {
            throw new OperationNotSupportedException("bool cannot used in greater than equal comparisons");
        }
        default:
        {
            throw new OperationNotSupportedException(lhsType + " cannot be used in greater than comparisons");
        }
        }
    }

    private void parseLessThanCompare(Attribute.Type lhsType, Attribute.Type rhsType, SiddhiGpu.ExecutorNode node)
    {
        switch (lhsType)
        {
        case STRING:
        {
            throw new OperationNotSupportedException("string cannot used in less than comparisons");
        }
        case INT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("int cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_INT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_INT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_INT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_INT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("int cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("int cannot be compared with " + rhsType);
            }
            break;
        }
        case LONG:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("long cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_LONG_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_LONG_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_LONG_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_LONG_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("long cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("long cannot be compared with " + rhsType);
            }
            break;
        }
        case FLOAT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("float cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_FLOAT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_FLOAT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_FLOAT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_FLOAT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("float cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("float cannot be compared with " + rhsType);
            }
            break;
        }
        case DOUBLE:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("double cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_DOUBLE_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_DOUBLE_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_DOUBLE_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LT_DOUBLE_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("double cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("double cannot be compared with " + rhsType);
            }
            break;
        }
        case BOOL:
        {
            throw new OperationNotSupportedException("bool cannot used in less than comparisons");
        }
        default:
        {
            throw new OperationNotSupportedException(lhsType + " cannot be used in less than comparisons");
        }
        }
    }

    private void parseLessThanEqualCompare(Attribute.Type lhsType, Attribute.Type rhsType, SiddhiGpu.ExecutorNode node)
    {
        switch (lhsType)
        {
        case STRING:
        {
            throw new OperationNotSupportedException("string cannot used in less than equal comparisons");
        }
        case INT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("int cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_INT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_INT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_INT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_INT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("int cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("int cannot be compared with " + rhsType);
            }
            break;
        }
        case LONG:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("long cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_LONG_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_LONG_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_LONG_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_LONG_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("long cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("long cannot be compared with " + rhsType);
            }
            break;
        }
        case FLOAT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("float cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_FLOAT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_FLOAT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_FLOAT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_FLOAT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("float cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("float cannot be compared with " + rhsType);
            }
            break;
        }
        case DOUBLE:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("double cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_DOUBLE_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_DOUBLE_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_DOUBLE_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_LE_DOUBLE_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("double cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("double cannot be compared with " + rhsType);
            }
            break;
        }
        case BOOL:
        {
            throw new OperationNotSupportedException("bool cannot used in less than equal comparisons");
        }
        default:
        {
            throw new OperationNotSupportedException(lhsType + " cannot be used in less than comparisons");
        }
        }
    }

    private void parseEqualCompare(Attribute.Type lhsType, Attribute.Type rhsType, SiddhiGpu.ExecutorNode node)
    {
        switch (lhsType)
        {
        case STRING:
        {
            switch (rhsType)
            {
            case STRING:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_STRING_STRING);
                break;
            default:
                throw new OperationNotSupportedException("sting cannot be compared with " + rhsType);
            }
            break;
        }
        case INT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("int cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_INT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_INT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_INT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_INT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("int cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("int cannot be compared with " + rhsType);
            }
            break;
        }
        case LONG:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("long cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_LONG_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_LONG_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_LONG_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_LONG_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("long cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("long cannot be compared with " + rhsType);
            }
            break;
        }
        case FLOAT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("float cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_FLOAT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_FLOAT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_FLOAT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_FLOAT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("float cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("float cannot be compared with " + rhsType);
            }
            break;
        }
        case DOUBLE:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("double cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_DOUBLE_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_DOUBLE_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_DOUBLE_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_DOUBLE_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("double cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("double cannot be compared with " + rhsType);
            }
            break;
        }
        case BOOL:
        {
            switch (rhsType)
            {
            case BOOL:
                node.SetConditionType(SiddhiGpu.EXECUTOR_EQ_BOOL_BOOL);
                break;
            default:
                throw new OperationNotSupportedException("bool cannot be compared with " + rhsType);
            }
            break;
        }
        default:
        {
            throw new OperationNotSupportedException(lhsType + " cannot be used in equal comparisons");
        }
        }
    }

    private void parseNotEqualCompare(Attribute.Type lhsType, Attribute.Type rhsType, SiddhiGpu.ExecutorNode node)
    {
        switch (lhsType)
        {
        case STRING:
        {
            switch (rhsType)
            {
            case STRING:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_STRING_STRING);
                break;
            default:
                throw new OperationNotSupportedException("sting cannot be compared with " + rhsType);
            }
            break;
        }
        case INT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("int cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_INT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_INT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_INT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_INT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("int cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("int cannot be compared with " + rhsType);
            }
            break;
        }
        case LONG:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("long cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_LONG_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_LONG_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_LONG_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_LONG_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("long cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("long cannot be compared with " + rhsType);
            }
            break;
        }
        case FLOAT:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("float cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_FLOAT_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_FLOAT_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_FLOAT_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_FLOAT_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("float cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("float cannot be compared with " + rhsType);
            }
            break;
        }
        case DOUBLE:
        {
            switch (rhsType)
            {
            case STRING:
                throw new OperationNotSupportedException("double cannot be compared with string");
            case INT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_DOUBLE_INT);
                break;
            case LONG:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_DOUBLE_LONG);
                break;
            case FLOAT:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_DOUBLE_FLOAT);
                break;
            case DOUBLE:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_DOUBLE_DOUBLE);
                break;
            case BOOL:
                throw new OperationNotSupportedException("double cannot be compared with bool");
            default:
                throw new OperationNotSupportedException("double cannot be compared with " + rhsType);
            }
            break;
        }
        case BOOL:
        {
            switch (rhsType)
            {
            case BOOL:
                node.SetConditionType(SiddhiGpu.EXECUTOR_NE_BOOL_BOOL);
                break;
            default:
                throw new OperationNotSupportedException("bool cannot be compared with " + rhsType);
            }
            break;
        }
        default:
        {
            throw new OperationNotSupportedException(lhsType + " cannot be used in not equal comparisons");
        }
        }
    }

    private void parseContainsCompare(Attribute.Type lhsType, Attribute.Type rhsType, SiddhiGpu.ExecutorNode node)
    {
        switch (lhsType)
        {
        case STRING:
        {
            switch (rhsType)
            {
            case STRING:
                node.SetConditionType(SiddhiGpu.EXECUTOR_CONTAINS);
                break;
            default:
                throw new OperationNotSupportedException(rhsType + " cannot be used in contains comparisons");
            }
            break;
        }
        default:
        {
            throw new OperationNotSupportedException(lhsType + " cannot be used in contains comparisons");
        }
        }
    }

    private Attribute.Type parseArithmeticOperationResultType(Attribute.Type lhsType, Attribute.Type rhsType)
    {
        if (lhsType == Attribute.Type.DOUBLE || rhsType == Attribute.Type.DOUBLE)
        {
            return Attribute.Type.DOUBLE;
        }
        else if (lhsType == Attribute.Type.FLOAT || rhsType == Attribute.Type.FLOAT)
        {
            return Attribute.Type.FLOAT;
        }
        else if (lhsType == Attribute.Type.LONG || rhsType == Attribute.Type.LONG)
        {
            return Attribute.Type.LONG;
        }
        else if (lhsType == Attribute.Type.INT || rhsType == Attribute.Type.INT)
        {
            return Attribute.Type.INT;
        }
        else
        {
            throw new ArithmeticException("Arithmetic operation between " + lhsType + " and "
                    + rhsType + " cannot be executed");
        }
    }
    
    private Attribute.Type parseVariable(Variable variable,
                                         MetaComplexEvent metaEvent, int currentState,
                                         List<SiddhiGpu.ExecutorNode> gpuFilterList,
                                         int parentNodeIndex) {
        
        String attributeName = variable.getAttributeName();
        int[] eventPosition = new int[2];
        int attributeIndex = -1;

        if (metaEvent instanceof MetaStreamEvent) {
            
            MetaStreamEvent metaStreamEvent = (MetaStreamEvent) metaEvent;
            AbstractDefinition abstractDefinition;
            Attribute.Type type;
            
            if (currentState == HAVING_STATE) {
                abstractDefinition = metaStreamEvent.getOutputStreamDefinition();
                type = abstractDefinition.getAttributeType(attributeName);
                attributeIndex = abstractDefinition.getAttributePosition(attributeName);
                eventPosition[STREAM_EVENT_CHAIN_INDEX] = HAVING_STATE;
            } else {
                abstractDefinition = metaStreamEvent.getLastInputDefinition();
                eventPosition[STREAM_EVENT_CHAIN_INDEX] = UNKNOWN_STATE;
                type = abstractDefinition.getAttributeType(attributeName);
                attributeIndex = abstractDefinition.getAttributePosition(attributeName);
            }
            
            int gpuDataType = SiddhiGpu.DataType.None;

            switch (type)
            {
            case BOOL:
                gpuDataType = SiddhiGpu.DataType.Boolean;
                break;
            case INT:
                gpuDataType = SiddhiGpu.DataType.Int;
                break;
            case LONG:
                gpuDataType = SiddhiGpu.DataType.Long;
                break;
            case FLOAT:
                gpuDataType = SiddhiGpu.DataType.Float;
                break;
            case DOUBLE:
                gpuDataType = SiddhiGpu.DataType.Double;
                break;
            case STRING:
                gpuDataType = SiddhiGpu.DataType.StringIn;
                break;
            case OBJECT:
                gpuDataType = SiddhiGpu.DataType.None;
                break;
            }

            gpuFilterList.add(new ExecutorNode().Init()
            .SetNodeType(SiddhiGpu.EXECUTOR_NODE_EXPRESSION)
            .SetExpressionType(SiddhiGpu.EXPRESSION_VARIABLE)
            .SetParentNode(parentNodeIndex)
            .SetVariableValue(new VariableValue().Init()
                    .SetStreamIndex(eventPosition[STREAM_EVENT_CHAIN_INDEX])
                    .SetDataType(gpuDataType)
                    .SetPosition(attributeIndex)));

            return type;
            
            
        } else { 
            
            MetaStateEvent metaStateEvent = (MetaStateEvent) metaEvent;
            Attribute.Type type = null;
            StreamDefinition streamDefinition = null;
            String firstInput = null;
            
            if (variable.getStreamId() == null) {
                MetaStreamEvent[] metaStreamEvents = metaStateEvent.getMetaStreamEvents();
                if (currentState == HAVING_STATE) {
                    streamDefinition = metaStateEvent.getOutputStreamDefinition();
                    try {
                        type = streamDefinition.getAttributeType(attributeName);
                        eventPosition[STREAM_EVENT_CHAIN_INDEX] = HAVING_STATE;
                        attributeIndex = streamDefinition.getAttributePosition(attributeName);
                    } catch (AttributeNotExistException e) {
                        currentState = UNKNOWN_STATE;
                    }
                }
                if (currentState == UNKNOWN_STATE) {
                    for (int i = 0; i < metaStreamEvents.length; i++) {
                        MetaStreamEvent metaStreamEvent = metaStreamEvents[i];
                        streamDefinition = (StreamDefinition) metaStreamEvent.getLastInputDefinition();
                        if (type == null) {
                            try {

                                type = streamDefinition.getAttributeType(attributeName);
                                firstInput = "Input Stream: " + streamDefinition.getId() + " with " +
                                        "reference: " + metaStreamEvent.getInputReferenceId();
                                eventPosition[STREAM_EVENT_CHAIN_INDEX] = i;
                                attributeIndex = streamDefinition.getAttributePosition(attributeName);
                            } catch (AttributeNotExistException e) {
                                // do nothing
                            }
                        } else {
                            try {
                                streamDefinition.getAttributeType(attributeName);
                                throw new ExecutionPlanValidationException(firstInput + " and Input Stream: " + streamDefinition.getId() + " with " +
                                        "reference: " + metaStreamEvent.getInputReferenceId() + " contains attributes with same" +
                                        " names ");
                            } catch (AttributeNotExistException e) {
                                // do nothing as its expected
                            }
                        }
                    }
                } else if (currentState >= 0) {
                    MetaStreamEvent metaStreamEvent = metaStreamEvents[currentState];
                    streamDefinition = (StreamDefinition) metaStreamEvent.getLastInputDefinition();
                    try {
                        type = streamDefinition.getAttributeType(attributeName);
                        eventPosition[STREAM_EVENT_CHAIN_INDEX] = currentState;
                        attributeIndex = streamDefinition.getAttributePosition(attributeName);
                    } catch (AttributeNotExistException e) {
                        throw new ExecutionPlanValidationException(e.getMessage() + " Input Stream: " +
                                streamDefinition.getId() + " with " + "reference: " + metaStreamEvent.getInputReferenceId());
                    }
                }
            } else {
                MetaStreamEvent[] metaStreamEvents = metaStateEvent.getMetaStreamEvents();
                for (int i = 0, metaStreamEventsLength = metaStreamEvents.length; i < metaStreamEventsLength; i++) {
                    MetaStreamEvent metaStreamEvent = metaStreamEvents[i];
                    streamDefinition = (StreamDefinition) metaStreamEvent.getLastInputDefinition();
                    
                    if (metaStreamEvent.getInputReferenceId() == null) {
                        if (streamDefinition.getId().equals(variable.getStreamId())) {
                            type = streamDefinition.getAttributeType(attributeName);
                            attributeIndex = streamDefinition.getAttributePosition(attributeName);
                            eventPosition[STREAM_EVENT_CHAIN_INDEX] = i;
                            break;
                        }
                    } else {
                        if (metaStreamEvent.getInputReferenceId().equals(variable.getStreamId())) {
                            type = streamDefinition.getAttributeType(attributeName);
                            attributeIndex = streamDefinition.getAttributePosition(attributeName);
                            eventPosition[STREAM_EVENT_CHAIN_INDEX] = i;
                            break;
                        }
                    }

                }
            }
            
            int gpuDataType = SiddhiGpu.DataType.None;

            switch (type)
            {
            case BOOL:
                gpuDataType = SiddhiGpu.DataType.Boolean;
                break;
            case INT:
                gpuDataType = SiddhiGpu.DataType.Int;
                break;
            case LONG:
                gpuDataType = SiddhiGpu.DataType.Long;
                break;
            case FLOAT:
                gpuDataType = SiddhiGpu.DataType.Float;
                break;
            case DOUBLE:
                gpuDataType = SiddhiGpu.DataType.Double;
                break;
            case STRING:
                gpuDataType = SiddhiGpu.DataType.StringIn;
                break;
            case OBJECT:
                gpuDataType = SiddhiGpu.DataType.None;
                break;
            }

            gpuFilterList.add(new ExecutorNode().Init()
            .SetNodeType(SiddhiGpu.EXECUTOR_NODE_EXPRESSION)
            .SetExpressionType(SiddhiGpu.EXPRESSION_VARIABLE)
            .SetParentNode(parentNodeIndex)
            .SetVariableValue(new VariableValue().Init()
                    .SetStreamIndex(eventPosition[STREAM_EVENT_CHAIN_INDEX])
                    .SetDataType(gpuDataType)
                    .SetPosition(attributeIndex)));

            return type;
        }
    }
}
