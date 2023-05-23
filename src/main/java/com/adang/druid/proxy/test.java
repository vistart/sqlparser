package com.adang.druid.proxy;


import com.adang.druid.proxy.sql.SQLUtils;
import com.adang.druid.proxy.sql.ast.SQLStatement;
import com.adang.druid.proxy.sql.ast.statement.SQLExprTableSource;
import com.adang.druid.proxy.sql.ast.statement.SQLTableSource;
import com.adang.druid.proxy.sql.dialect.mysql.parser.MySqlStatementParser;
import com.adang.druid.proxy.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.adang.druid.proxy.sql.parser.SQLStatementParser;
import com.adang.druid.proxy.util.JdbcConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class test {
//  public static void printSQLObject(SQLObject sqlObject) {
//    if (sqlObject instanceof SQLStatement) {
//      List<SQLObject> children = ((SQLStatement) sqlObject).getChildren();
//      for (SQLObject child : children){
//        //visit
//      }
//    } else if (sqlObject instanceof SQLSelect) {
//      SQLSelectQuery query = ((SQLSelect) sqlObject).getQuery();
//      System.out.println(query.toString());
//    } else if (sqlObject instanceof SQLSelectQuery) {
//    }
//  }

  public static class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {
    private Map<String, SQLTableSource> aliasMap = new HashMap<>();
    private Map<SQLTableSource, Integer> hashCodeMap = new HashMap<>();
//    public boolean visit(SQLExprTableSource x) {
//      String alias = x.getAlias();
//      aliasMap.put(alias, x);
//      return true;
//    }

    public Map<String, SQLTableSource> getAliasMap() {
      return aliasMap;
    }

    public boolean visit(SQLExprTableSource x) {
      hashCodeMap.put(x, x.hashCode());
      return true;
    }

    public Map<SQLTableSource, Integer> getHashCodeMap() {
      return hashCodeMap;
    }
  }

  public static void main(String[] args) {

   /* String sql = "SELECT UUID();";

    // parser得到AST
    SQLStatementParser parser = new MySqlStatementParser(sql);
    List<SQLStatement> stmtList = parser.parseStatementList(); //

    // 将AST通过visitor输出
    StringBuilder out = new StringBuilder();
    MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);

    for (SQLStatement stmt : stmtList) {
      stmt.accept(visitor);
      out.append(";");
    }

    System.out.println(out.toString());*/

    String sql = """
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

    // 新建 MySQL Parser
    SQLStatementParser parser = new MySqlStatementParser(sql);

//    // 使用Parser解析生成AST，这里SQLStatement就是AST
//    SQLStatement statement = parser.parseStatement();
//
//    // 使用visitor来访问AST
//    MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
//    statement.accept(visitor);
//
//    // 从visitor中拿出你所关注的信息
//    System.out.println(visitor.getColumns());

    final String dbType = JdbcConstants.MYSQL;
    List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
    for (SQLStatement stmt: stmtList) {
      System.out.println(SQLUtils.toSQLString(stmt));
    }
  }
}
