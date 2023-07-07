package com.adang.druid.proxy;

public class feature {
    public int height;
    public int totalChildren;
    // 是否有子节点
    public boolean hasChildren = false;
    // 是否有表达式表
    public boolean hasExprTableSource = false;
    // 是否有连接表
    public boolean hasJoinTableSource = false;
    // 是否有子查询表
    public boolean hasSubqueryTableSource = false;
    // 是否有 with 查询子句
    public boolean hasWithSubqueryClause = false;
    // select 查询是否有 with 查询子句
    public boolean hasSelectWithSubquery = false;
    // 是否有 union 查询
    public boolean hasUnionQuery = false;
    // 是否有插入值
    public int hasInsertValues = 0;
    // 是否有更新项
    public int hasUpdateSetItems = 0;
    // 是否有列定义
    public int hasColumnDefinitions = 0;
    // 是否有 mysql 唯一约束
    public int hasMySqlUnique = 0;
    // 是否有二元运算
    public int hasBinaryOps = 0;

    public feature() {
        totalChildren = 0;
        height = 1;
    }

    public String toString() {
        return "feature{" +
                "height=" + height +
                ", totalChildren=" + totalChildren +
                '}';
    }

    public feature add(feature t) {
        //totalChildren += t.totalChildren;
        //height += t.height;
        hasChildren |= t.hasChildren;
        hasExprTableSource |= t.hasExprTableSource;
        hasJoinTableSource |= t.hasJoinTableSource;
        hasSubqueryTableSource |= t.hasSubqueryTableSource;
        hasWithSubqueryClause |= t.hasWithSubqueryClause;
        hasSelectWithSubquery |= t.hasSelectWithSubquery;
        hasUnionQuery |= t.hasUnionQuery;
        hasInsertValues += t.hasInsertValues;
        hasUpdateSetItems += t.hasUpdateSetItems;
        hasColumnDefinitions += t.hasColumnDefinitions;
        hasMySqlUnique += t.hasMySqlUnique;
        hasBinaryOps += t.hasBinaryOps;
        return this;
    }

    public feature addTotalChildren(feature t) {
        totalChildren += t.totalChildren;
        return this; // for chaining
    }

    public feature addHeight(feature t) {
        height += t.height;
        return this; // for chaining
    }
}
