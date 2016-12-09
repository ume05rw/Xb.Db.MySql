using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using MySql.Data.MySqlClient;

namespace Xb.Db
{
    /// <summary>
    /// MySQL用DB接続管理クラス
    /// </summary>
    /// <remarks>
    /// Xb.Dbクラスを継承して実装。
    /// 主な処理が親クラスに書いてあり、ロジックが行き来するので注意。
    /// </remarks>
    public class Mysql : Xb.Db.DbBase
    {

        protected new MySqlConnection Connection;
        protected new string TranCmdBegin = "START TRANSACTION";
        protected new string SqlFind = "SELECT * FROM {0} WHERE {1} LIMIT 1 ";


        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="name"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <param name="address"></param>
        /// <param name="isBuildStructureModels"></param>
        /// <param name="encodingType"></param>
        /// <param name="additionalString"></param>
        /// <remarks></remarks>
        public Mysql(string name,
            string user = "root",
            string password = "",
            string address = "localhost",
            bool isBuildStructureModels = true,
            Xb.Db.DbBase.EncodeType encodingType = EncodeType.Utf8,
            string additionalString = "")
            : base(name,
                user,
                password,
                address,
                encodingType,
                additionalString)
        {
            //基底クラス側の値を上書きする。
            base.TranCmdBegin = this.TranCmdBegin;
            base.SqlFind = this.SqlFind;
            base._encoding = this.Encoding;

            if (isBuildStructureModels)
            {
                //接続したDBの構造データを取得する。
                this.GetStructure();
            }
        }


        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="name"></param>
        /// <param name="isBuildStructureModels"></param>
        /// <param name="encodingType"></param>
        /// <remarks></remarks>
        public Mysql(MySqlConnection connection,
            string name,
            bool isBuildStructureModels = true,
            Xb.Db.DbBase.EncodeType encodingType = EncodeType.Utf8)
        {
            this._name = name;
            this.Connection = connection;
            this.EncodingType = encodingType;
            //this.SetConnection((System.Data.Common.DbConnection) this.Connection);

            string encString = "utf8";
            switch (this.EncodingType)
            {
                case EncodeType.Utf8:
                    encString = "utf8";
                    break;
                case EncodeType.Sjis:
                    encString = "sjis";
                    break;
                case EncodeType.Eucjp:
                    encString = "ujis";
                    break;
            }

            //エンコードを変更する。
            //※SET NAMESにはSQLインジェクション脆弱性がある。Webアプリでは使用しないように。
            this.Execute("SET NAMES " + encString);
            this.Execute("SET CHARACTER SET " + encString);

            //トランザクションを初期化
            this._isInTransaction = false;

            //コネクション参照を規定クラス側に渡す。
            base.Connection = this.Connection;

            //基底クラス側の値を上書きする。
            base.TranCmdBegin = this.TranCmdBegin;
            base.TranCmdCommit = this.TranCmdCommit;
            base.TranCmdRollback = this.TranCmdRollback;
            base.SqlFind = this.SqlFind;

            if (isBuildStructureModels)
            {
                //接続したDBの構造データを取得する。
                this.GetStructure();
            }
        }


        /// <summary>
        /// DBへ接続する
        /// </summary>>
        /// <remarks></remarks>
        protected override void Open()
        {
            //コネクション変数の状態チェック-既に接続済みのとき、何もしない。
            if (this.IsConnected)
                return;

            string encString = "utf8";
            switch (this.EncodingType)
            {
                case EncodeType.Utf8:
                    encString = "utf8";
                    break;
                case EncodeType.Sjis:
                    encString = "sjis";
                    break;
                case EncodeType.Eucjp:
                    encString = "ujis";
                    break;
            }

            //接続設定を文字列でセット
            string connectionString =
                string.Format("server={0};user id={1}; password={2}; database={3}; charset={4}; pooling=false{5}",
                    this._address,
                    this._user,
                    this._password,
                    this._name,
                    encString,
                    string.IsNullOrEmpty(this._additionalConnectionString)
                        ? ""
                        : "; " + this._additionalConnectionString);

            try
            {
                //広域宣言したコネクション変数を使って、ＤＢへ接続
                this.Connection = new MySqlConnection(connectionString);
                this.Connection.Open();

                //エンコードを変更する。
                //※SET NAMESにはSQLインジェクション脆弱性がある。Webアプリでは使用しないように。
                this.Execute("SET NAMES " + encString);
                this.Execute("SET CHARACTER SET " + encString);
                this.Execute("SET SET CLIENT ENCODING " + encString);

                //this.SetConnection((System.Data.Common.DbConnection) this.Connection);

            }
            catch (Exception)
            {
                this.Connection = null;
            }

            if (!this.IsConnected)
            {
                Xb.Util.Out("Xb.Db.MySql.Open: DB接続に失敗しました。");
                throw new ApplicationException("DB接続に失敗しました。");
            }

            //トランザクションを初期化
            this._isInTransaction = false;

        }


        /// <summary>
        /// 接続先DBの構造を取得する。
        /// </summary>
        /// <remarks></remarks>

        protected override void GetStructure()
        {
            System.Text.StringBuilder sql = null;
            DataTable dt = null;

            //テーブルリストを取得する。
            sql = new System.Text.StringBuilder();
            sql.AppendFormat("\r\n SELECT ");
            sql.AppendFormat("\r\n     TABLE_NAME ");
            sql.AppendFormat("\r\n FROM ");
            sql.AppendFormat("\r\n     information_schema.TABLES ");
            sql.AppendFormat("\r\n WHERE ");
            sql.AppendFormat("\r\n     TABLE_SCHEMA = '{0}' ", this._name);
            sql.AppendFormat("\r\n ORDER BY ");
            sql.AppendFormat("\r\n     TABLE_NAME ");
            dt = this.Query(sql.ToString());

            this._tableNames = new List<string>();
            foreach (DataRow row in dt.Rows)
            {
                this._tableNames.Add(row["TABLE_NAME"].ToString().ToUpper());
            }

            //カラム情報を取得する。
            sql = new System.Text.StringBuilder();
            sql.AppendFormat("\r\n SELECT ");
            sql.AppendFormat("\r\n      UCASE(TABLE_NAME) AS TABLE_NAME ");
            sql.AppendFormat("\r\n     ,ORDINAL_POSITION AS COLUMN_INDEX ");
            sql.AppendFormat("\r\n     ,COLUMN_NAME AS COLUMN_NAME ");
            sql.AppendFormat("\r\n     ,DATA_TYPE AS 'TYPE' ");
            sql.AppendFormat("\r\n     ,CHARACTER_MAXIMUM_LENGTH AS CHAR_LENGTH ");
            sql.AppendFormat("\r\n     ,NUMERIC_PRECISION AS NUM_PREC ");
            sql.AppendFormat("\r\n     ,CASE ");
            sql.AppendFormat("\r\n         WHEN NUMERIC_SCALE IS NOT NULL THEN NUMERIC_SCALE ");
            sql.AppendFormat("\r\n         WHEN NUMERIC_PRECISION IS NOT NULL THEN 0 ");
            sql.AppendFormat("\r\n         ELSE NULL ");
            sql.AppendFormat("\r\n      END AS NUM_SCALE ");
            sql.AppendFormat("\r\n     ,CASE WHEN COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END AS IS_PRIMARY_KEY ");
            sql.AppendFormat("\r\n     ,CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS IS_NULLABLE ");
            sql.AppendFormat("\r\n     ,COLUMN_COMMENT AS COMMENT ");
            sql.AppendFormat("\r\n FROM ");
            sql.AppendFormat("\r\n     information_schema.COLUMNS ");
            sql.AppendFormat("\r\n WHERE ");
            sql.AppendFormat("\r\n     TABLE_SCHEMA = '{0}' ", this._name);
            sql.AppendFormat("\r\n ORDER BY ");
            sql.AppendFormat("\r\n      TABLE_NAME ASC ");
            sql.AppendFormat("\r\n     ,ORDINAL_POSITION ASC ");
            dt = this.Query(sql.ToString());
            this._structureTable = dt;

            //テーブルごとのモデルインスタンスを生成・保持しておく。
            this.BuildModels();
        }


        /// <summary>
        /// 文字列項目のクォートラップ処理
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        /// <remarks>
        /// 標準をSqlServer/SQLite風クォートにセット。MySQLではOverrideする。
        /// </remarks>
        public override string Quote(string text)
        {
            return Xb.Str.MySqlQuote(text);
        }


        /// <summary>
        /// SQL文でコマンドを実行する(結果を返さない)
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override int Execute(string sql)
        {
            base.Command = new MySqlCommand(sql, this.Connection);
            return base.Execute(sql);
        }


        /// <summary>
        /// SQL文でクエリを実行し、結果を返す
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override DataTable Query(string sql)
        {
            base.Adapter = new MySqlDataAdapter(sql, this.Connection);
            return base.Query(sql);
        }

        /// <summary>
        /// SQL文でクエリを実行し、結果を返す
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="dt"></param>
        /// <remarks></remarks>
        public override void Fill(string sql, DataTable dt)
        {
            base.Adapter = new MySqlDataAdapter(sql, this.Connection);
            base.Fill(sql, dt);
        }

        /// <summary>
        /// データベースのバックアップファイルを取得する。
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override bool BackupDb(string fileName)
        {
            throw new InvalidOperationException("Not Implemented.");

            ////渡し値パスが実在することを確認する。
            //if (!base.RemoveIfExints(fileName))
            //    return false;

            //string mysqlPath = "";
            //Xb.File.Node node = default(Xb.File.Node);

            //if (Xb.Base.Is32Bit)
            //{
            //    mysqlPath = Xb.App.Path.ProgramFiles + "\\MySQL";
            //    if (!System.IO.Directory.Exists(mysqlPath))
            //    {
            //        Xb.Util.Out("Xb.Db.MySql.BackupDb: MySQLインストール先が不明です。");
            //        throw new ApplicationException("Xb.Db.MySql.BackupDb: MySQLインストール先が不明です。");
            //    }
            //}
            //else if (Xb.Base.Is64Bit)
            //{
            //    mysqlPath = Xb.App.Path.ProgramFiles + "\\MySQL";
            //    if (!System.IO.Directory.Exists(mysqlPath))
            //    {
            //        mysqlPath = Xb.App.Path.ProgramFilesX86 + "\\MySQL";
            //        if (!System.IO.Directory.Exists(mysqlPath))
            //        {
            //            Xb.Util.Out("Xb.Db.MySql.BackupDb: MySQLインストール先が不明です。");
            //            throw new ApplicationException("Xb.Db.MySql.BackupDb: MySQLインストール先が不明です。");
            //        }
            //    }
            //}

            //node = Xb.File.Util.GetListRecursive(mysqlPath).Find("mysqldump.exe");
            //if (node == null)
            //{
            //    Xb.Util.Out("Xb.Db.MySql.BackupDb: mysqldump.exeが見付かりません。");
            //    throw new ApplicationException("Xb.Db.MySql.BackupDb: mysqldump.exeが見付かりません。");
            //}

            //try
            //{
            //    //DBバックアップを実行する。
            //    System.Diagnostics.Process ps = new System.Diagnostics.Process();

            //    ps.StartInfo.FileName = node.Path;
            //    ps.StartInfo.Arguments =
            //        string.Format("--user='{0}' {1}--default-character-set=sjis --force --result-file=\"{3}\" {2} ",
            //            this._user,
            //            string.IsNullOrEmpty(this._password)
            //                ? ""
            //                : "--password='" + this._password + "' ",
            //            this._name,
            //            fileName);

            //    //出力を読み取れるようにする
            //    ps.StartInfo.UseShellExecute = false;
            //    ps.StartInfo.RedirectStandardOutput = true;
            //    ps.StartInfo.RedirectStandardInput = false;

            //    //ウィンドウを表示しないようにする
            //    ps.StartInfo.CreateNoWindow = true;

            //    //Xb.App.Out("Backup Start")
            //    ps.Start();

            //    //標準出力の結果を取得する。
            //    Console.Write(ps.StandardOutput.ReadToEnd());

            //    ps.WaitForExit();
            //    ps.Close();
            //    //Xb.App.Out("Backup Complete")

            //}
            //catch (Exception)
            //{
            //    Xb.Util.Out("Xb.Db.MySql.BackupDb: バックアップ実行に失敗しました。");
            //    throw new ApplicationException("バックアップ実行に失敗しました。");
            //}

            //return true;
        }
    }
}