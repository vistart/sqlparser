package com.adang.druid.proxy;


import com.adang.druid.proxy.sql.SQLUtils;
import com.adang.druid.proxy.sql.ast.SQLExpr;
import com.adang.druid.proxy.sql.ast.SQLObject;
import com.adang.druid.proxy.sql.ast.SQLOrderBy;
import com.adang.druid.proxy.sql.ast.SQLStatement;
import com.adang.druid.proxy.sql.ast.expr.*;
import com.adang.druid.proxy.sql.ast.statement.*;
import com.adang.druid.proxy.sql.dialect.mysql.ast.MySqlUnique;
import com.adang.druid.proxy.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.adang.druid.proxy.sql.dialect.mysql.parser.MySqlStatementParser;
import com.adang.druid.proxy.sql.parser.SQLStatementParser;
import com.adang.druid.proxy.util.JdbcConstants;

import java.util.List;

public class test {

  public static short analyseStatementType(SQLStatement stmt) {
    short statementType = 0; // 未分类
    if (stmt instanceof SQLSelectStatement) {
      System.out.println("select");
      statementType = 1;
    } else if (stmt instanceof SQLInsertStatement) {
      System.out.println("insert");
      statementType = 2;
    } else if (stmt instanceof SQLUpdateStatement) {
      System.out.println("update");
      statementType = 3;
    } else if (stmt instanceof SQLDeleteStatement) {
      System.out.println("delete");
      statementType = 4;
    } else if (stmt instanceof SQLCreateTableStatement) {
      System.out.println("create table");
      statementType = 5;
    } else if (stmt instanceof SQLAlterStatement) {
      System.out.println("alter");
      statementType = 6;
    } else if (stmt instanceof SQLBlockStatement) {
      System.out.println("block");
      statementType = 7;
    } else if (stmt instanceof SQLCallStatement) {
      System.out.println("call");
      statementType = 8;
    } else if (stmt instanceof SQLCloseStatement) {
      System.out.println("close");
      statementType = 9;
    } else if (stmt instanceof SQLCommentStatement) {
      System.out.println("comment");
      statementType = 10;
    } else if (stmt instanceof SQLCommitStatement) {
      System.out.println("commit");
      statementType = 11;
    }  else if (stmt instanceof SQLCreateStatement) {
      System.out.println("create");
      statementType = 12;
    } else if (stmt instanceof SQLDeclareStatement) {
      System.out.println("declare");
      statementType = 13;
    } else if (stmt instanceof SQLDescribeStatement) {
      System.out.println("describe");
      statementType = 14;
    } else if (stmt instanceof SQLDropStatement) {
      System.out.println("drop");
      statementType = 15;
    } else if (stmt instanceof SQLExplainStatement) {
      System.out.println("explain");
      statementType = 16;
    } else if (stmt instanceof SQLExprStatement) {
      System.out.println("expr");
      statementType = 17;
    } else if (stmt instanceof SQLFetchStatement) {
      System.out.println("fetch");
      statementType = 18;
    } else if (stmt instanceof SQLGrantStatement) {
      System.out.println("grant");
      statementType = 19;
    } else if (stmt instanceof SQLIfStatement) {
      System.out.println("if");
      statementType = 20;
    } else if (stmt instanceof SQLLoopStatement) {
      System.out.println("loop");
      statementType = 21;
    } else if (stmt instanceof SQLMergeStatement) {
      System.out.println("merge");
      statementType = 22;
    } else if (stmt instanceof SQLOpenStatement) {
      System.out.println("open");
      statementType = 23;
    } else if (stmt instanceof SQLReplaceStatement) {
      System.out.println("replace");
      statementType = 24;
    } else if (stmt instanceof SQLReturnStatement) {
      System.out.println("return");
      statementType = 25;
    } else if (stmt instanceof SQLRollbackStatement) {
      System.out.println("rollback");
      statementType = 26;
    } else if (stmt instanceof SQLSetStatement) {
      System.out.println("set");
      statementType = 27;
    } else if (stmt instanceof SQLTruncateStatement) {
      System.out.println("truncate");
      statementType = 28;
    } else if (stmt instanceof SQLUseStatement) {
      System.out.println("use");
      statementType = 29;
    } else if (stmt instanceof SQLWhileStatement) {
      System.out.println("while");
      statementType = 30;
    } else if (stmt instanceof SQLCaseStatement) {
      System.out.println("case");
      statementType = 31;
    } else if (stmt instanceof SQLRevokeStatement) {
      System.out.println("revoke");
      statementType = 32;
    } else if (stmt instanceof SQLAlterFunctionStatement) {
      System.out.println("alter function");
      statementType = 33;
    } else if (stmt instanceof SQLAlterTypeStatement) {
      System.out.println("alter type");
      statementType = 34;
    } else {
      System.out.println(stmt.getClass());
    }
    return statementType;
  }

  public static feature analyseTotalChildrenSQLBinaryOpExpr(SQLBinaryOpExpr sql) {
    feature feature = new feature();
    int height = 1;
    if (sql.getLeft() instanceof SQLBinaryOpExpr) {
      feature t = analyseTotalChildrenSQLBinaryOpExpr((SQLBinaryOpExpr) sql.getLeft());
      feature = feature.addTotalChildren(t);
      height = Math.max(height, t.height+1);
      feature = feature.add(t);
    } else {
      feature.totalChildren++;
    }
    if (sql.getRight() instanceof SQLBinaryOpExpr) {
      feature t = analyseTotalChildrenSQLBinaryOpExpr((SQLBinaryOpExpr) sql.getRight());
      feature = feature.addTotalChildren(t);
      height = Math.max(height, t.height+1);
      feature = feature.add(t);
    } else {
      feature.totalChildren++;
    }
    feature.height = height;
    return feature;
  }

  public static feature analyseTotalChildrenSQLExprTableSource(SQLExprTableSource from) {
    feature feature = new feature();
    if (from.getExpr() instanceof SQLIdentifierExpr) {
      feature.totalChildren++;
    }
    return feature;
  }

  public static feature analyseTotalChildrenSQLSelectQuery(SQLSelect sql) {
    feature feature = new feature();
    int height = 1;
    SQLSelectQuery query = sql.getQuery();
    if (query instanceof MySqlSelectQueryBlock) {
      // select list
      if (((MySqlSelectQueryBlock) query).getSelectList().size() > 0) {
        for (SQLSelectItem selectItem : ((MySqlSelectQueryBlock) query).getSelectList()) {
          if (selectItem.getExpr() instanceof SQLIdentifierExpr || selectItem.getExpr() instanceof SQLAggregateExpr) {
            feature.totalChildren++;
          }
          if (selectItem.getExpr() instanceof SQLCaseExpr) {
            if (selectItem.getExpr() != null && ((SQLCaseExpr) selectItem.getExpr()).getItems().size() > 0){
              feature.totalChildren += ((SQLCaseExpr) selectItem.getExpr()).getItems().size();
            }
          }
          if (selectItem.getExpr() instanceof SQLMethodInvokeExpr) {
            if (selectItem.getExpr() != null && ((SQLMethodInvokeExpr) selectItem.getExpr()).getParameters().size() > 0) {
              feature.totalChildren += ((SQLMethodInvokeExpr) selectItem.getExpr()).getParameters().size();
            }
          }
        }
      }

      // from
      SQLTableSource from = ((MySqlSelectQueryBlock)query).getFrom();
      if (from instanceof SQLSubqueryTableSource) {
        SQLSelect subSelect = ((SQLSubqueryTableSource) from).getSelect();
        feature t = analyseTotalChildrenSQLSelectQuery(subSelect);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (from instanceof SQLExprTableSource) {
        feature t = analyseTotalChildrenSQLExprTableSource((SQLExprTableSource) from);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }

      // group by
      SQLSelectGroupByClause groupBy = ((MySqlSelectQueryBlock) query).getGroupBy();
      if (groupBy != null && groupBy.getItems().size() > 0) {
        for (SQLExpr item : groupBy.getItems()) {
          if (item instanceof SQLIdentifierExpr) {
            feature.totalChildren++;
          }
        }
      }

      // order by
      SQLOrderBy orderBy = ((MySqlSelectQueryBlock) query).getOrderBy();
      if (orderBy != null && orderBy.getItems().size() > 0) {
        for (SQLSelectOrderByItem item : orderBy.getItems()) {
          if (item.getExpr() instanceof SQLIdentifierExpr) {
            feature.totalChildren++;
          }
        }
      }

      // where
      SQLExpr where = ((MySqlSelectQueryBlock) query).getWhere();
      if (where instanceof SQLBinaryOpExpr) {
        feature t = analyseTotalChildrenSQLBinaryOpExpr((SQLBinaryOpExpr) where);
        // TODO should we include a height of 1 for these cases?
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
    }
    feature.height = height;
    return feature;
  }

  public static feature analyseTotalChildrenSQLUpdateStatement(SQLUpdateStatement sql) {
    feature feature = new feature();
    System.out.println(sql);
    return feature;
  }

  public static feature analyseTotalChildrenSQLUpdateSetItem(SQLUpdateSetItem sql) {
    feature feature = new feature();
    if (sql.getColumn() instanceof SQLIdentifierExpr) {
      feature.totalChildren++;
    }
    if (sql.getValue() instanceof SQLCharExpr) {
      feature.totalChildren++;
    }
    return feature;
  }

  public static feature analyseTotalChildrenSQLInsertStatement(SQLInsertStatement sql) {
    feature feature = new feature();
    System.out.println(sql);
    return feature;
  }

  public static feature analyseTotalChildrenSQLDeleteStatement(SQLDeleteStatement sql) {
    feature feature = new feature();
    System.out.println(sql);
    return feature;
  }

  public static feature analyseStmt(SQLStatement stmt) {
    feature feature = new feature();
    if (stmt instanceof SQLSelectStatement){
      feature t = analyseTotalChildren(stmt);
      feature = feature.addTotalChildren(t);
      feature = feature.addHeight(t);
      feature = feature.add(t);
    } else if (stmt instanceof SQLUpdateStatement sqlUpdateStatement) {
      feature t = analyseTotalChildren(stmt);
      feature = feature.addTotalChildren(t);
      feature = feature.addHeight(t);
      feature = feature.add(t);
    } else if (stmt instanceof SQLInsertStatement insertStatement) {
      feature t = analyseTotalChildren(stmt);
      feature = feature.addTotalChildren(t);
      feature = feature.addHeight(t);
      feature = feature.add(t);
    } else if(stmt instanceof SQLDeleteStatement deleteStatement) {
      feature t = analyseTotalChildren(stmt);
      feature = feature.addTotalChildren(t);
      feature = feature.addHeight(t);
      feature = feature.add(t);
    } else if (stmt instanceof SQLCreateTableStatement createTableStmt) {
      feature t = analyseTotalChildren(stmt);
      feature = feature.addTotalChildren(t);
      feature = feature.addHeight(t);
      feature = feature.add(t);
    } else if (stmt instanceof SQLDropTableStatement dropTableStmt) {
      feature t = analyseTotalChildren(stmt);
      feature = feature.addTotalChildren(t);
      feature = feature.addHeight(t);
      feature = feature.add(t);
    } else if (stmt instanceof SQLCreateViewStatement createViewStmt) {
      feature t = analyseTotalChildren(stmt);
      feature = feature.addTotalChildren(t);
      feature = feature.addHeight(t);
      feature = feature.add(t);
    } else if (stmt instanceof SQLCreateIndexStatement createIndexStmt) {
      feature t = analyseTotalChildren(stmt);
      feature = feature.addTotalChildren(t);
      feature = feature.addHeight(t);
      feature = feature.add(t);
    }
    return feature;
  }

  public static feature analyseTotalChildren(SQLStatement stmt) {
    feature feature = new feature();
    if (stmt.getChildren().isEmpty()) {
      return feature;
    }
    feature.hasChildren = true;
    int height = 1;
    for (SQLObject child : stmt.getChildren()) {
      if (child instanceof SQLStatement) {
        feature t = analyseTotalChildren((SQLStatement) child);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (child instanceof SQLSelect) {
        feature t = analyseTotalChildrenSQLSelectQuery((SQLSelect) child);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (child instanceof SQLUpdateStatement) {
        // feature t = analyseTotalChildrenSQLUpdateStatement((SQLUpdateStatement) child);
        feature t= analyseTotalChildren((SQLStatement) child);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (child instanceof SQLUpdateSetItem) {
        feature t = analyseTotalChildrenSQLUpdateSetItem((SQLUpdateSetItem) child);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (child instanceof SQLInsertStatement) {
        // feature t = analyseTotalChildrenSQLInsertStatement((SQLInsertStatement) child);
        feature t = analyseTotalChildren((SQLStatement) child);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (child instanceof SQLDeleteStatement) {
        // feature t = analyseTotalChildrenSQLDeleteStatement((SQLDeleteStatement) child);
        feature t = analyseTotalChildren((SQLStatement) child);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (child instanceof SQLExprTableSource) {
        feature t = analyseTotalChildrenSQLExprTableSource((SQLExprTableSource) child);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (child instanceof SQLIdentifierExpr) {
        feature.totalChildren++;
      }
      if (child instanceof SQLBinaryOpExpr) {
        feature t = analyseTotalChildrenSQLBinaryOpExpr((SQLBinaryOpExpr) child);
        feature = feature.addTotalChildren(t);
        height = Math.max(height, t.height+1);
        feature = feature.add(t);
      }
      if (child instanceof SQLColumnDefinition || child instanceof SQLUnionQuery) {
        feature.totalChildren++;
      }
      feature.hasExprTableSource |= child instanceof SQLExprTableSource;
      feature.hasJoinTableSource |= child instanceof SQLJoinTableSource;
      feature.hasSubqueryTableSource |= child instanceof SQLSubqueryTableSource;
      feature.hasWithSubqueryClause |= child instanceof  SQLWithSubqueryClause;

      short statementType = analyseStatementType(stmt);
      if (statementType == 1 && child instanceof SQLSelect) { // SELECT 语句独有特征
        SQLWithSubqueryClause subquery = ((SQLSelect) child).getWithSubQuery();
        feature.hasSelectWithSubquery |= subquery != null;
        if (((SQLSelect) child).getQuery() instanceof SQLSelectQueryBlock query) {
          SQLObject from = query.getFrom();
          feature.hasExprTableSource |= from instanceof SQLExprTableSource;
          feature.hasJoinTableSource |= from instanceof SQLJoinTableSource;
          feature.hasSubqueryTableSource |= from instanceof SQLSubqueryTableSource;
          feature.hasWithSubqueryClause |= from instanceof  SQLWithSubqueryClause;
        } else if (((SQLSelect) child).getQuery() instanceof SQLUnionQuery) {
          feature.hasUnionQuery = true;
          // SQLUnionQuery query = (SQLUnionQuery) ((SQLSelect) child).getQuery();
        }
      }

      if (statementType == 2 && child instanceof SQLInsertStatement.ValuesClause) {
        feature.hasInsertValues = ((SQLInsertStatement.ValuesClause) child).getValues().size();
      }

      if (statementType == 3 && child instanceof SQLUpdateSetItem) {
        feature.hasUpdateSetItems++;
      }

      if (statementType == 4) {
        System.out.println(child);
      }

      if (statementType == 5) {
        if (child instanceof SQLColumnDefinition) {
          feature.hasColumnDefinitions++;
          continue;
        }
        if (child instanceof MySqlUnique) {
          feature.hasMySqlUnique++;
        }
      }

      if (child instanceof SQLBinaryOpExpr) {
        feature.hasBinaryOps++;
      }
    }
    feature.height = height;
    return feature;
  }

  public static void analyseStatement(SQLStatement stmt) {
    short statementType = analyseStatementType(stmt);
    // 是否有子节点
    boolean hasChildren = false;
    // 是否有表达式表
    boolean hasExprTableSource = false;
    // 是否有连接表
    boolean hasJoinTableSource = false;
    // 是否有子查询表
    boolean hasSubqueryTableSource = false;
    // 是否有 with 查询子句
    boolean hasWithSubqueryClause = false;
    // select 查询是否有 with 查询子句
    boolean hasSelectWithSubquery = false;
    // 是否有 union 查询
    boolean hasUnionQuery = false;
    // 是否有插入值
    int hasInsertValues = 0;
    // 是否有更新项
    int hasUpdateSetItems = 0;
    // 是否有列定义
    int hasColumnDefinitions = 0;
    // 是否有 mysql 唯一约束
    int hasMySqlUnique = 0;
    // 是否有二元运算
    int hasBinaryOps = 0;

    int totalChildren = 0;

    int height = 0;

    if (!stmt.getChildren().isEmpty()) {
      hasChildren = true;

      List<SQLObject> children = stmt.getChildren();
      System.out.println(children);
      for (SQLObject child: children) {
        hasExprTableSource |= child instanceof SQLExprTableSource;
        hasJoinTableSource |= child instanceof SQLJoinTableSource;
        hasSubqueryTableSource |= child instanceof SQLSubqueryTableSource;
        hasWithSubqueryClause |= child instanceof  SQLWithSubqueryClause;

        if (statementType == 1 && child instanceof SQLSelect) { // SELECT 语句独有特征
          SQLWithSubqueryClause subquery = ((SQLSelect) child).getWithSubQuery();
          hasSelectWithSubquery |= subquery != null;
          if (((SQLSelect) child).getQuery() instanceof SQLSelectQueryBlock query) {
            SQLObject from = query.getFrom();
            hasExprTableSource |= from instanceof SQLExprTableSource;
            hasJoinTableSource |= from instanceof SQLJoinTableSource;
            hasSubqueryTableSource |= from instanceof SQLSubqueryTableSource;
            hasWithSubqueryClause |= from instanceof  SQLWithSubqueryClause;
          } else if (((SQLSelect) child).getQuery() instanceof SQLUnionQuery) {
            hasUnionQuery = true;
            // SQLUnionQuery query = (SQLUnionQuery) ((SQLSelect) child).getQuery();
          }
        }

        if (statementType == 2 && child instanceof SQLInsertStatement.ValuesClause) {
          hasInsertValues = ((SQLInsertStatement.ValuesClause) child).getValues().size();
        }

        if (statementType == 3 && child instanceof SQLUpdateSetItem) {
          hasUpdateSetItems++;
        }

        if (statementType == 4) {
          System.out.println(child);
        }

        if (statementType == 5) {
          if (child instanceof SQLColumnDefinition) {
            hasColumnDefinitions++;
            continue;
          }
          if (child instanceof MySqlUnique) {
            hasMySqlUnique++;
          }
        }

        if (child instanceof SQLBinaryOpExpr) {
          hasBinaryOps++;
        }
      }
    }

    feature t = analyseStmt(stmt);

    System.out.println(t);
  }

  public static void analyse(String sql) {

    // 新建 MySQL Parser
    SQLStatementParser parser = new MySqlStatementParser(sql);
    final String dbType = JdbcConstants.MYSQL;
    List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
    for (SQLStatement stmt: stmtList) {
      analyseStatement(stmt);
    }
  }

  public static void main(String[] args) {
    String[] sqls = new String[7];
    sqls[0] = """
            SELECT `type`, `review_interval_distrib`, COUNT(*) FROM (
                SELECT *, CASE WHEN `A`.`review_interval` <= 300 THEN `0_5m`
                WHEN `A`.`review_interval` > 300 AND `A`.`review_interval` <= 600 THEN `5_10m`
                WHEN `A`.`review_interval` > 600 THEN `greater_10m` END AS `review_interval_distrib`
                FROM (
                    SELECT `type`, `review_person`, TIMESTAMPDIFF(SECOND, `create_time`, `review_time`) AS `review_interval`
                    FROM `review` WHERE DATE(`create_time`) = '2023-05-21' AND (`status` = 2 or `status` = 3)
                ) AS `A`
            ) AS `B` GROUP BY `type`, `review_person`, `review_interval_distrib` ORDER BY `type`, `review_person`
            """;
    sqls[1] = """
            INSERT INTO `review` (`type`,`create_time`) VALUES (1, '2023-05-25 00:00:00')
            """;
    sqls[2] = """
            UPDATE `review` SET `update_time` = '2023-05-25 12:00:00' WHERE `id` = 1
            """;
    sqls[3] = """
            DELETE FROM `review` WHERE `update_time` > '2022-05-25 23:59:59'
            """;
    sqls[4] = """
create table go_rush_producer.node_info
(
    id           bigint unsigned auto_increment comment '节点编号'
        primary key,
    name         varchar(255)      default ''                   not null comment '节点名称（由节点自行提供）',
    node_version varchar(255)      default ''                   not null comment '节点版本号（x.y.z）或（x.y.z.build）或（git commit no 不少于十二位）',
    host         varchar(255)      default ''                   not null comment '节点套接字的域（ip/domain）',
    port         smallint unsigned default '8080'               not null comment '节点套接字的端口',
    level        tinyint unsigned                               not null comment '节点级别（0-master，1-slave）',
    superior_id  bigint unsigned   default '0'                  not null comment '上级ID。0表示没有上级。',
    turn         int unsigned      default '0'                  not null comment '上级主节点失效后的接替顺序（数值越小优先级越高）',
    created_at   timestamp(3)      default CURRENT_TIMESTAMP(3) not null comment '创建时间',
    updated_at   timestamp(3)      default CURRENT_TIMESTAMP(3) not null on update CURRENT_TIMESTAMP(3) comment '最后更新时间',
    version      bigint unsigned   default '0'                  not null comment '本条记录版本。从0开始。',
    constraint node_level_superior_turn_index
        unique (level, superior_id, turn) comment '节点级别、上级节点和接替顺序',
    constraint node_socket_index
        unique (host, port) comment '节点套接字索引'
)
    comment '节点信息';

            """;
    sqls[5] = """
create index node_info_id_index
    on go_rush_producer.node_info (id);
            """;
    sqls[6] = """
            SELECT `A`.`hour`, `value`, `value_yesterday`, `avg_7_days` FROM
            (SELECT `hour`, `value` FROM `indicator_cycle`
            WHERE (`day` = '2023-07-03') AND `is_delete` = 0 AND `cate_1` = 'user' AND `cate_2` = '' AND `cate_3` ='' AND `name` = 'total'
            ORDER BY `hour`) AS `A` LEFT JOIN\s
            (SELECT `hour`, `value` as `value_yesterday` FROM `indicator_cycle`
            WHERE `day` = DATE(DATE_SUB('2023-07-03', INTERVAL 1 DAY)) AND `is_delete` = 0 AND `cate_1` = 'user' AND `cate_2` = '' AND `cate_3` ='' AND `name` = 'total'
            ORDER BY `hour`) AS `B` ON `A`.`hour` = `B`.`hour` LEFT JOIN
            (SELECT `hour`, AVG(`value`) as `avg_7_days` FROM `indicator_cycle`
            WHERE `day` BETWEEN DATE(DATE_SUB('2023-07-03', INTERVAL 6 DAY)) AND '2023-07-03' AND `is_delete` = 0 AND `cate_1` = 'user' AND `cate_2` = '' AND `cate_3` ='' AND `name` = 'total'
            GROUP BY `hour` ORDER BY `hour`) AS `C`
            ON `A`.`hour` = `C`.`hour`
                        
            limit 0, 5000
            """;
    for (String sql : sqls) {
      analyse(sql);
    }
  }
}
