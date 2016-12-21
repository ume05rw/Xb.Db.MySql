using System;
using System.Text;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;
using TestXb.Db;
using Xb.Db;

namespace TestXb
{
    [TestClass]
    public class MySqlTests : MySqlBase
    {
        public MySqlTests() : base(false)
        {
        }

        [TestMethod()]
        public void ConstructorTest()
        {
            this.Out("CreateTest Start.");
            var db = this._dbDirect;

            Assert.AreEqual(MySqlBase.Server, db.Address);
            Assert.AreEqual(MySqlBase.NameTarget, db.Name);
            Assert.AreEqual(MySqlBase.UserId, db.User);

            Assert.AreEqual(Encoding.UTF8, db.Encoding);
            Assert.AreEqual(Xb.Db.DbBase.StringSizeCriteriaType.Length, db.StringSizeCriteria);

            Assert.IsFalse(db.IsInTransaction);

            //モデル生成フラグがOFFなので、テーブル構造を取得していない。
            Assert.AreEqual(0, db.TableNames.Length);
            Assert.AreEqual(0, db.Models.Count);


            db = this._dbRef;
            Assert.IsNotNull(db.Connection);
            Assert.AreEqual("", db.Address);
            Assert.AreEqual(MySqlBase.NameTarget, db.Name);
            Assert.AreEqual("", db.User);

            Assert.AreEqual(Encoding.UTF8, db.Encoding);
            Assert.AreEqual(Xb.Db.DbBase.StringSizeCriteriaType.Length, db.StringSizeCriteria);

            Assert.IsFalse(db.IsInTransaction);

            //モデル生成フラグがOFFなので、テーブル構造を取得していない。
            Assert.AreEqual(0, db.TableNames.Length);
            Assert.AreEqual(0, db.Models.Count);

            db.Dispose();
            this.Out("CreateTest End.");
        }

        [TestMethod()]
        public void QuoteTest()
        {
            this.Out("QuoteTest Start.");
            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                var db = (constructorType == 0)
                    ? this._dbDirect
                    : this._dbRef;

                Assert.AreEqual("'hello'", db.Quote("hello"));
                Assert.AreEqual("'\\'hello\\''", db.Quote("'hello'"));
                Assert.AreEqual("'hel\\'lo'", db.Quote("hel'lo"));

                Assert.AreEqual("'hello'", db.Quote("hello", DbBase.LikeMarkPosition.None));
                Assert.AreEqual("'hello%'", db.Quote("hello", DbBase.LikeMarkPosition.After));
                Assert.AreEqual("'%hello'", db.Quote("hello", DbBase.LikeMarkPosition.Before));
                Assert.AreEqual("'%hello%'", db.Quote("hello", DbBase.LikeMarkPosition.Both));
                Assert.AreEqual("'\\'hello\\'%'", db.Quote("'hello'", DbBase.LikeMarkPosition.After));
                Assert.AreEqual("'%hel\\'lo'", db.Quote("hel'lo", DbBase.LikeMarkPosition.Before));
            }

            this.Out("QuoteTest End.");
        }


        [TestMethod()]
        public void GetParameterTest()
        {
            this.Out("GetParameterTest Start.");
            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                var db = (constructorType == 0)
                    ? this._dbDirect
                    : this._dbRef;

                var param = db.GetParameter("@name", "value", DbType.String);
                Assert.AreEqual("@name", param.ParameterName);
                Assert.AreEqual("value", param.Value);
                Assert.AreEqual(DbType.String, ((MySqlParameter) param).DbType);
                Assert.AreEqual(ParameterDirection.Input, param.Direction);

                param = db.GetParameter("name", "value", DbType.String);
                Assert.AreEqual("@name", param.ParameterName);
                Assert.AreEqual("value", param.Value);
                Assert.AreEqual(DbType.String, ((MySqlParameter) param).DbType);
                Assert.AreEqual(ParameterDirection.Input, param.Direction);

                param = db.GetParameter();
                Assert.AreEqual("", param.ParameterName);
                Assert.AreEqual(null, param.Value);
                Assert.AreEqual(DbType.String, ((MySqlParameter) param).DbType);
                Assert.AreEqual(ParameterDirection.Input, param.Direction);
            }
            this.Out("GetParameterTest End.");
        }


        [TestMethod()]
        public async Task ExecuteTest()
        {
            this.Out("ExecuteTest Start.");
            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    this.InitTables();

                    //初期状態
                    var rt = (asyncType == 0)
                        ? db.Query("SELECT * FROM Test WHERE COL_STR = 'KEY' ")
                        : await db.QueryAsync("SELECT * FROM Test WHERE COL_STR = 'KEY' ");
                    Assert.AreEqual(1, rt.Rows.Count);
                    Assert.AreEqual("KEY", rt.Rows[0]["COL_STR"]);
                    Assert.AreEqual((decimal)0, rt.Rows[0]["COL_DEC"]);
                    Assert.AreEqual(DBNull.Value, rt.Rows[0]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2000-12-31"), rt.Rows[0]["COL_DATETIME"]);

                    //INSERT SQL文字列のみ
                    int cnt;
                    var sql = string.Format(
                        "INSERT INTO {0} "
                            + "(COL_STR, COL_DEC, COL_INT, COL_DATETIME) "
                            + " VALUES ({1}, {2}, {3}, {4});"
                        , "test2", "'123'", 0.123, 1234567, "'2020-01-01'");
                    cnt = (asyncType == 0)
                            ? db.Execute(sql)
                            : await db.ExecuteAsync(sql);
                    Assert.AreEqual(1, cnt);

                    rt = (asyncType == 0)
                        ? db.Query("SELECT * FROM Test2 WHERE COL_STR = '123' ")
                        : await db.QueryAsync("SELECT * FROM Test2 WHERE COL_STR = '123' ");
                    Assert.AreEqual(1, rt.Rows.Count);
                    Assert.AreEqual("123", rt.Rows[0]["COL_STR"]);
                    Assert.AreEqual((decimal)0.123, rt.Rows[0]["COL_DEC"]);
                    Assert.AreEqual(1234567, rt.Rows[0]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2020-01-01"), rt.Rows[0]["COL_DATETIME"]);

                    //INSERT DbParameter使用

                    sql = "INSERT INTO Test2 "
                              + "(COL_STR, COL_DEC, COL_INT, COL_DATETIME) "
                              + " VALUES (@str, @dec, @int, @datetime);";
                    var aryParams = new DbParameter[]
                                    {
                                        db.GetParameter("str", "KEY2")
                                        , db.GetParameter("dec", 98.76, DbType.Decimal)
                                        , db.GetParameter("int", 999, DbType.Int32)
                                        , db.GetParameter("datetime", "1999-12-31", DbType.DateTime)
                                    };

                    cnt = (asyncType == 0)
                            ? db.Execute(sql, aryParams)
                            : await db.ExecuteAsync(sql, aryParams);
                    Assert.AreEqual(1, cnt);

                    rt = (asyncType == 0)
                        ? db.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY2' ")
                        : await db.QueryAsync("SELECT * FROM Test2 WHERE COL_STR = 'KEY2' ");
                    Assert.AreEqual(1, rt.Rows.Count);
                    Assert.AreEqual("KEY2", rt.Rows[0]["COL_STR"]);
                    Assert.AreEqual((decimal)98.76, rt.Rows[0]["COL_DEC"]);
                    Assert.AreEqual(999, rt.Rows[0]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("1999-12-31"), rt.Rows[0]["COL_DATETIME"]);

                    //UPDATE SQL文字列のみ
                    sql = "UPDATE Test SET COL_DEC=10, COL_INT=20, COL_DATETIME=NULL WHERE COL_STR LIKE '%B%' ";
                    cnt = (asyncType == 0)
                            ? db.Execute(sql)
                            : await db.ExecuteAsync(sql);

                    Assert.AreEqual(4, cnt);
                    rt = (asyncType == 0)
                        ? db.Query("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ")
                        : await db.QueryAsync("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ");
                    Assert.AreEqual(4, rt.Rows.Count);
                    foreach (var row in rt.Rows)
                    {
                        Assert.IsTrue(row["COL_STR"].ToString().IndexOf("B") != -1);
                        Assert.AreEqual((decimal)10, row["COL_DEC"]);
                        Assert.AreEqual(20, row["COL_INT"]);
                        Assert.AreEqual(DBNull.Value, row["COL_DATETIME"]);
                    }

                    //UPDATE DbParameter使用
                    sql = "UPDATE Test SET COL_DEC=@dec, COL_INT=@int, COL_DATETIME=@datetime WHERE COL_STR = @str ";
                    aryParams = new DbParameter[]
                    {
                        db.GetParameter("@str", "KEY")
                        , db.GetParameter("@dec", DBNull.Value, DbType.Decimal)
                        , db.GetParameter("@int", 9876, DbType.Int32)
                        , db.GetParameter("@datetime", DateTime.Parse("2200/03/04"), DbType.DateTime)
                    };
                    cnt = (asyncType == 0)
                            ? db.Execute(sql, aryParams)
                            : await db.ExecuteAsync(sql, aryParams);
                    Assert.AreEqual(1, cnt);

                    rt = (asyncType == 0)
                        ? db.Query("SELECT * FROM Test WHERE COL_STR = 'KEY' ")
                        : await db.QueryAsync("SELECT * FROM Test WHERE COL_STR = 'KEY' ");
                    Assert.AreEqual(1, rt.Rows.Count);
                    Assert.AreEqual("KEY", rt.Rows[0]["COL_STR"]);
                    Assert.AreEqual(DBNull.Value, rt.Rows[0]["COL_DEC"]);
                    Assert.AreEqual(9876, rt.Rows[0]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2200-03-04"), rt.Rows[0]["COL_DATETIME"]);

                    //DELETE SQL文字列のみ
                    sql = "DELETE FROM Test2 WHERE COL_STR = 'KEY' ";
                    cnt = (asyncType == 0)
                            ? db.Execute(sql)
                            : await db.ExecuteAsync(sql);
                    Assert.AreEqual(1, cnt);

                    rt = (asyncType == 0)
                        ? db.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY' ")
                        : await db.QueryAsync("SELECT * FROM Test2 WHERE COL_STR = 'KEY' ");
                    Assert.AreEqual(0, rt.Rows.Count);

                    //UPDATE DbParameter使用
                    sql = "DELETE FROM Test2 WHERE COL_STR = @str ";
                    aryParams = new DbParameter[] { db.GetParameter("str", "KEY2") };
                    cnt = (asyncType == 0)
                            ? db.Execute(sql, aryParams)
                            : await db.ExecuteAsync(sql, aryParams);
                    Assert.AreEqual(1, cnt);

                    rt = (asyncType == 0)
                        ? db.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY2' ")
                        : await db.QueryAsync("SELECT * FROM Test2 WHERE COL_STR = 'KEY2' ");
                    Assert.AreEqual(0, rt.Rows.Count);
                }
            }
            this.Out("ExecuteTest End.");
        }

        [TestMethod()]
        public async Task GetReaderTest()
        {
            this.Out("GetReaderTest Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    this.InitTables();

                    var sql = "SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ";
                    var reader = (asyncType == 0)
                                    ? db.GetReader(sql)
                                    : await db.GetReaderAsync(sql);

                    Assert.IsTrue(reader.HasRows);
                    Assert.AreEqual(4, reader.FieldCount);
                    Assert.AreEqual("COL_STR", reader.GetName(0));
                    Assert.AreEqual("COL_DEC", reader.GetName(1));
                    Assert.AreEqual("COL_INT", reader.GetName(2));
                    Assert.AreEqual("COL_DATETIME", reader.GetName(3));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(0, reader.GetOrdinal("COL_STR"));
                    Assert.AreEqual(1, reader.GetOrdinal("COL_DEC"));
                    Assert.AreEqual(2, reader.GetOrdinal("COL_INT"));
                    Assert.AreEqual(3, reader.GetOrdinal("COL_DATETIME"));

                    Assert.AreEqual("ABC", reader.GetString(0));
                    Assert.AreEqual((decimal)1, reader.GetDecimal(1));
                    Assert.AreEqual(1, reader.GetInt32(2));
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("ABC", reader.GetString(0));
                    Assert.AreEqual((decimal)1, reader.GetDecimal(1));
                    Assert.AreEqual(1, reader.GetInt32(2));
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("ABC", reader.GetString(0));
                    Assert.AreEqual((decimal)1, reader.GetDecimal(1));
                    Assert.AreEqual(1, reader.GetInt32(2));
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("BB", reader.GetString(0));
                    Assert.AreEqual((decimal)12.345, reader.GetDecimal(1));
                    Assert.AreEqual(12345, reader.GetInt32(2));
                    Assert.AreEqual(DateTime.Parse("2016-12-13"), reader.GetDateTime(3));
                    Assert.IsFalse(reader.Read());

                    reader.Dispose();
                }
            }

            this.Out("GetReaderTest End.");
        }


        [TestMethod()]
        public async Task QueryTest()
        {
            this.Out("QueryTest Start.");
            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    this.InitTables();

                    ResultTable rt = null;
                    var sql = "SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ";
                    rt = (asyncType == 0)
                            ? db.Query(sql)
                            : await db.QueryAsync(sql);

                    Assert.AreEqual(4, rt.ColumnCount);
                    Assert.AreEqual(4, rt.RowCount);
                    Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
                    Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
                    Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
                    Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
                    Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
                    Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
                    Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
                    Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));

                    Assert.AreEqual("ABC", rt.Rows[0]["COL_STR"]);
                    Assert.AreEqual((decimal)1, rt.Rows[0]["COL_DEC"]);
                    Assert.AreEqual(1, rt.Rows[0]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0]["COL_DATETIME"]);
                    Assert.AreEqual("ABC", rt.Rows[0][0]);
                    Assert.AreEqual((decimal)1, rt.Rows[0][1]);
                    Assert.AreEqual(1, rt.Rows[0][2]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0][3]);

                    Assert.AreEqual("ABC", rt.Rows[1]["COL_STR"]);
                    Assert.AreEqual((decimal)1, rt.Rows[1]["COL_DEC"]);
                    Assert.AreEqual(1, rt.Rows[1]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1]["COL_DATETIME"]);
                    Assert.AreEqual("ABC", rt.Rows[1][0]);
                    Assert.AreEqual((decimal)1, rt.Rows[1][1]);
                    Assert.AreEqual(1, rt.Rows[1][2]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1][3]);

                    Assert.AreEqual("ABC", rt.Rows[2]["COL_STR"]);
                    Assert.AreEqual((decimal)1, rt.Rows[2]["COL_DEC"]);
                    Assert.AreEqual(1, rt.Rows[2]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2]["COL_DATETIME"]);
                    Assert.AreEqual("ABC", rt.Rows[2][0]);
                    Assert.AreEqual((decimal)1, rt.Rows[2][1]);
                    Assert.AreEqual(1, rt.Rows[2][2]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2][3]);

                    Assert.AreEqual("BB", rt.Rows[3]["COL_STR"]);
                    Assert.AreEqual((decimal)12.345, rt.Rows[3]["COL_DEC"]);
                    Assert.AreEqual(12345, rt.Rows[3]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3]["COL_DATETIME"]);
                    Assert.AreEqual("BB", rt.Rows[3][0]);
                    Assert.AreEqual((decimal)12.345, rt.Rows[3][1]);
                    Assert.AreEqual(12345, rt.Rows[3][2]);
                    Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3][3]);

                    sql = "SELECT * FROM Test WHERE COL_STR = 'NO-MATCH-ROW' ORDER BY COL_STR ";
                    rt = (asyncType == 0)
                            ? db.Query(sql)
                            : await db.QueryAsync(sql);

                    Assert.IsFalse(rt == null);
                    Assert.AreEqual(4, rt.ColumnCount);
                    Assert.AreEqual(0, rt.RowCount);
                    Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
                    Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
                    Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
                    Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
                    Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
                    Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
                    Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
                    Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));
                }
            }
            this.Out("QueryTest End.");
        }


        [TestMethod()]
        public async Task QueryTTest()
        {
            this.Out("QueryTTest Start.");
            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    this.InitTables();

                    var sql = "SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ";
                    var classRows = (asyncType == 0)
                                        ? db.Query<TestTableType>(sql)
                                        : await db.QueryAsync<TestTableType>(sql);


                    Assert.AreEqual(4, classRows.Length);

                    Assert.AreEqual("ABC", classRows[0].COL_STR);
                    Assert.AreEqual((decimal)1, classRows[0].COL_DEC);
                    Assert.AreEqual(1, classRows[0].COL_INT);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[0].COL_DATETIME);

                    Assert.AreEqual("ABC", classRows[1].COL_STR);
                    Assert.AreEqual((decimal)1, classRows[1].COL_DEC);
                    Assert.AreEqual(1, classRows[1].COL_INT);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[1].COL_DATETIME);

                    Assert.AreEqual("ABC", classRows[2].COL_STR);
                    Assert.AreEqual((decimal)1, classRows[2].COL_DEC);
                    Assert.AreEqual(1, classRows[2].COL_INT);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[2].COL_DATETIME);

                    Assert.AreEqual("BB", classRows[3].COL_STR);
                    Assert.AreEqual((decimal)12.345, classRows[3].COL_DEC);
                    Assert.AreEqual(12345, classRows[3].COL_INT);
                    Assert.AreEqual(DateTime.Parse("2016-12-13"), classRows[3].COL_DATETIME);

                    sql = "SELECT * FROM Test WHERE COL_STR = 'NO-MATCH-ROW' ORDER BY COL_STR ";
                    classRows = (asyncType == 0)
                                    ? db.Query<TestTableType>(sql)
                                    : await db.QueryAsync<TestTableType>(sql);
                    Assert.AreEqual(0, classRows.Length);
                }
            }

            this.Out("QueryTTest End.");
        }
        private class TestTableType
        {
            public string COL_STR { get; set; }
            public decimal COL_DEC { get; set; }
            public int COL_INT { get; set; }
            public DateTime COL_DATETIME { get; set; }
        }


        [TestMethod()]
        public async Task FindTest()
        {
            this.Out("FindTest Start.");
            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    this.InitTables();

                    var rr = (asyncType == 0)
                        ? db.Find("test3", "COL_STR = 'ABC'")
                        : await db.FindAsync("test3", "COL_STR = 'ABC'");
                    Assert.IsFalse(rr == null);
                    Assert.AreEqual("ABC", rr["COL_STR"]);
                    Assert.IsTrue((new int[] { 1, 2, 3 }).Contains((int)rr["COL_INT"]));
                    Assert.AreEqual((decimal)1, rr["COL_DEC"]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rr["COL_DATETIME"]);

                    rr = (asyncType == 0)
                        ? db.Find("test", "COL_DATETIME = '2000-12-31'")
                        : await db.FindAsync("test", "COL_DATETIME = '2000-12-31'");
                    Assert.IsFalse(rr == null);
                    Assert.AreEqual("KEY", rr["COL_STR"]);
                    Assert.AreEqual(DBNull.Value, rr["COL_INT"]);
                    Assert.AreEqual((decimal)0, rr["COL_DEC"]);
                    Assert.AreEqual(DateTime.Parse("2000-12-31"), rr["COL_DATETIME"]);

                    rr = (asyncType == 0)
                        ? db.Find("test", "COL_DATETIME = '2000-12-30'")
                        : await db.FindAsync("test", "COL_DATETIME = '2000-12-30'");
                    Assert.IsTrue(rr == null);
                }
            }

            this.Out("FindTest End.");
        }


        [TestMethod()]
        public async Task FindAllTest()
        {
            this.Out("FindAllTest Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    this.InitTables();

                    var rt = (asyncType == 0)
                                ? db.FindAll("test", "COL_STR LIKE '%B%'", "COL_STR")
                                : await db.FindAllAsync("test", "COL_STR LIKE '%B%'", "COL_STR");

                    //Query("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
                    Assert.AreEqual(4, rt.ColumnCount);
                    Assert.AreEqual(4, rt.RowCount);
                    Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
                    Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
                    Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
                    Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
                    Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
                    Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
                    Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
                    Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));

                    Assert.AreEqual("ABC", rt.Rows[0]["COL_STR"]);
                    Assert.AreEqual((decimal)1, rt.Rows[0]["COL_DEC"]);
                    Assert.AreEqual(1, rt.Rows[0]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0]["COL_DATETIME"]);
                    Assert.AreEqual("ABC", rt.Rows[0][0]);
                    Assert.AreEqual((decimal)1, rt.Rows[0][1]);
                    Assert.AreEqual(1, rt.Rows[0][2]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0][3]);

                    Assert.AreEqual("ABC", rt.Rows[1]["COL_STR"]);
                    Assert.AreEqual((decimal)1, rt.Rows[1]["COL_DEC"]);
                    Assert.AreEqual(1, rt.Rows[1]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1]["COL_DATETIME"]);
                    Assert.AreEqual("ABC", rt.Rows[1][0]);
                    Assert.AreEqual((decimal)1, rt.Rows[1][1]);
                    Assert.AreEqual(1, rt.Rows[1][2]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1][3]);

                    Assert.AreEqual("ABC", rt.Rows[2]["COL_STR"]);
                    Assert.AreEqual((decimal)1, rt.Rows[2]["COL_DEC"]);
                    Assert.AreEqual(1, rt.Rows[2]["COL_INT"]);

                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2]["COL_DATETIME"]);
                    Assert.AreEqual("ABC", rt.Rows[2][0]);
                    Assert.AreEqual((decimal)1, rt.Rows[2][1]);
                    Assert.AreEqual(1, rt.Rows[2][2]);
                    Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2][3]);

                    Assert.AreEqual("BB", rt.Rows[3]["COL_STR"]);
                    Assert.AreEqual((decimal)12.345, rt.Rows[3]["COL_DEC"]);
                    Assert.AreEqual(12345, rt.Rows[3]["COL_INT"]);
                    Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3]["COL_DATETIME"]);
                    Assert.AreEqual("BB", rt.Rows[3][0]);
                    Assert.AreEqual((decimal)12.345, rt.Rows[3][1]);
                    Assert.AreEqual(12345, rt.Rows[3][2]);
                    Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3][3]);

                    rt = (asyncType == 0)
                        ? db.FindAll("test", "COL_STR = 'NO-MATCH-ROW' ", "COL_STR")
                        : await db.FindAllAsync("test", "COL_STR = 'NO-MATCH-ROW' ", "COL_STR");

                    Assert.IsFalse(rt == null);
                    Assert.AreEqual(4, rt.ColumnCount);
                    Assert.AreEqual(0, rt.RowCount);
                    Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
                    Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
                    Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
                    Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
                    Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
                    Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
                    Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
                    Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));
                }
            }

            this.Out("FindAllTest End.");
        }


        [TestMethod()]
        public async Task TransactionAsyncTest()
        {
            this.Out("TransactionTest Start.");
            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    this.InitTables();

                    ResultTable rt;

                    if (asyncType == 0)
                    {
                        db.BeginTransaction();
                    }
                    else
                    {
                        await db.BeginTransactionAsync();
                    }

                    var sql = string.Format(
                                "INSERT INTO {0} "
                                    + "(COL_STR, COL_DEC, COL_INT, COL_DATETIME) "
                                    + " VALUES ({1}, {2}, {3}, {4}); "
                                , "test2", "'123'", 0.123, 1234567, "'2020-01-01'");
                    var cnt = (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql);
                    Assert.AreEqual(1, cnt);

                    sql = "SELECT * FROM Test2 WHERE COL_STR = '123' ";
                    rt = (asyncType == 0)
                        ? db.Query(sql)
                        : await db.QueryAsync(sql);
                    Assert.AreEqual(1, rt.RowCount);



                    if (asyncType == 0)
                    {
                        db.RollbackTransaction();
                    }
                    else
                    {
                        await db.RollbackTransactionAsync();
                    }

                    sql = "SELECT * FROM Test2 WHERE COL_STR = '123' ";
                    rt = (asyncType == 0)
                        ? db.Query(sql)
                        : await db.QueryAsync(sql);

                    Assert.AreEqual(0, rt.RowCount);

                    if (asyncType == 0)
                    {
                        db.BeginTransaction();
                    }
                    else
                    {
                        await db.BeginTransactionAsync();
                    }

                    sql = string.Format("INSERT INTO {0} "
                                            + " (COL_STR, COL_DEC, COL_INT, COL_DATETIME) "
                                            + " VALUES ({1}, {2}, {3}, {4});"
                                        , "test2", "'123'", 0.123, 1234567, "'2020-01-01'");
                    cnt = (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql);
                    Assert.AreEqual(1, cnt);

                    if (asyncType == 0)
                    {
                        db.CommitTransaction();
                    }
                    else
                    {
                        await db.CommitTransactionAsync();
                    }

                    sql = "SELECT * FROM Test2 WHERE COL_STR = '123' ";
                    rt = (asyncType == 0)
                        ? db.Query(sql)
                        : await db.QueryAsync(sql);
                    Assert.AreEqual(1, rt.RowCount);
                }
            }

            this.Out("TransactionTest End.");
        }
    }
}
