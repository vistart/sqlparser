package com.adang.druid.proxy;


import com.adang.druid.proxy.sql.SQLUtils;
import com.adang.druid.proxy.sql.ast.SQLObject;
import com.adang.druid.proxy.sql.ast.SQLStatement;
import com.adang.druid.proxy.sql.ast.statement.*;
import com.adang.druid.proxy.sql.dialect.mysql.parser.MySqlStatementParser;
import com.adang.druid.proxy.sql.parser.SQLStatementParser;
import com.adang.druid.proxy.util.JdbcConstants;

import java.util.List;

public class test {

  public static void analyseStatement(SQLStatement stmt) {
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
    } else {
      System.out.println(stmt.getClass());
    }
    boolean hasChildren = false;
    boolean hasExprTableSource = false;
    boolean hasJoinTableSource = false;
    boolean hasSubqueryTableSource = false;
    boolean hasWithSubqueryClause = false;
    boolean hasSelectWithSubquery = false;
    boolean hasUnionQuery = false;
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
          if (((SQLSelect) child).getQuery() instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock query = (SQLSelectQueryBlock) ((SQLSelect) child).getQuery();
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
      }
    }


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
    String[] sqls = new String[5];
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

create index node_info_id_index
    on go_rush_producer.node_info (id);


            """;
    for (String sql : sqls) {
      analyse(sql);
    }
  }
}
