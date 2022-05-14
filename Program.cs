using System;
using MySql.Data.MySqlClient;   //引用MySql
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DataBaseConnect
{
    class Program
    {
        //文件路径
        //连接数据库
        //连接的表名
        //连接的库名

        static string rootPath = @"C:\Users\le\Desktop\revit_data";//我的
        static string constructorString = "server=localhost;UserId=root;password=huangjl;Database=db_revit"; //我的
        static string tableName = "t_task";  //我的
        static string databaseName = "db_revit";  //我的

        //static string rootPath = @"C:\revit_data";//服务器
        //static string constructorString = "server=localhost;User Id=root;password=szu@2022;Database=db_revit";  //服务器
        //static string tableName = "t_task";  //服务器
        //static string databaseName = "db_revit";  //服务器

        //static string rootPath = @"C:\revit_data";//师兄
        //static string constructorString = "server=localhost;User Id=root;password=root;Database=test";  //师兄
        //static string tableName = "t_revit";  //师兄
        //static string databaseName = "test";  //师兄

        static void Main(string[] args) 
        {
            //测试
            //Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}");
            //return;

            if (args.Length >= 4) 
            {
                //自定义
                rootPath = args[0];
                constructorString = args[1];
                tableName = args[2];
                databaseName = args[3];
            }
            else if (args.Length > 0) 
            {
                Console.WriteLine($"参数不足4个 不启动程序.");
                Console.WriteLine($"默认参数: \"{rootPath}\" \"{constructorString}\" \"{tableName}\" \"{databaseName}\"");
                return;
            }

            //D2OMain();
            Thread thread_D2O = new Thread(D2OMain);
            thread_D2O.IsBackground = false;
            Console.WriteLine("线程 data -> order 启动.");
            thread_D2O.Start();

            Thread.Sleep(TimeSpan.FromSeconds(2));

            Thread thread_O2O = new Thread(O2OMain);
            thread_O2O.IsBackground = false;
            Console.WriteLine("线程 order_new -> result 启动.");
            thread_O2O.Start();

            //Thread.Sleep(TimeSpan.FromDays(9));
        }

        static void D2OMain()
        {
            //连接
            MySqlConnection myConnnect = new MySqlConnection(constructorString);
            myConnnect.Open();

            while (true)
            {
                //等待
                Thread.Sleep(TimeSpan.FromSeconds(5));
                Console.WriteLine("\t苏醒,执行 data -> order.");
                MySqlCommand myCmd;

                //命令
                String cmdStr = $"SELECT task_id FROM {databaseName}.{tableName} WHERE step=1 AND status=0 LIMIT 1;";
                myCmd = new MySqlCommand(cmdStr, myConnnect);

                //执行
                MySqlDataReader mysqldr = myCmd.ExecuteReader();

                //返回结果
                if (mysqldr.Read())//mysqldr.Read()返回的是bool值，意在判断是否有下一条数据
                {
                    //读取基本信息
                    Console.WriteLine("读取返回结果:");
                    string id = mysqldr["task_id"].ToString();
                    Console.WriteLine("任务id="+ id);
                    //关闭命令连接
                    mysqldr.Close();

                    //错误情况
                    try 
                    { 
                        //文件路径
                        string readPath =Path.Combine(rootPath, id, "revit_data.xml");
                        string savePath =Path.Combine(rootPath, id, "revit_order.xml");
                        //调用dll
                        RevitDataXml2RevitOrderXml.MainProgram d2o = new RevitDataXml2RevitOrderXml.MainProgram(new String[] { readPath, savePath });

                        //完成,执行返回命令
                        Console.WriteLine("执行成功!");
                        //命令
                        string new_cmdStr = $"update {tableName} set `step`=2,`update_time`=\"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}\" where `task_id`={id};";
                        myCmd = new MySqlCommand(new_cmdStr, myConnnect);

                        //执行并关闭
                        mysqldr = myCmd.ExecuteReader();
                        mysqldr.Close();
                    }
                    catch(Exception e) 
                    {
                        //失败,执行返回命令
                        Console.WriteLine("执行异常:" + e.ToString());
                        //命令
                        string new_cmdStr = $"update {tableName} set `status`=2,`progress_info`=\"{e.Message}\",`update_time`=\"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}\" where `task_id`={id};";
                        myCmd = new MySqlCommand(new_cmdStr, myConnnect);

                        //执行并关闭
                        mysqldr = myCmd.ExecuteReader();
                        mysqldr.Close();
                    }
                }
                else
                    mysqldr.Close();
            }
            //结束 never gonna say goodbye
            //myConnnect.Close();
        }

        static void O2OMain()
        {
            //连接
            MySqlConnection myConnnect = new MySqlConnection(constructorString);
            myConnnect.Open();

            while (true)
            {
                //等待
                Thread.Sleep(TimeSpan.FromSeconds(5));
                Console.WriteLine("\t苏醒,执行 order_new -> result.");
                MySqlCommand myCmd;

                //命令
                String cmdStr = $"SELECT task_id FROM {databaseName}.{tableName} WHERE step=3 AND status=0 LIMIT 1;";
                myCmd = new MySqlCommand(cmdStr, myConnnect);

                //执行
                MySqlDataReader mysqldr = myCmd.ExecuteReader();

                //返回结果
                if (mysqldr.Read())//mysqldr.Read()返回的是bool值，意在判断是否有下一条数据
                {
                    //读取基本信息
                    Console.WriteLine("读取返回结果:");
                    string id = mysqldr["task_id"].ToString();
                    Console.WriteLine("任务id=" + id);
                    //关闭命令连接
                    mysqldr.Close();

                    //错误情况
                    try
                    {
                        //文件路径
                        string readPath = Path.Combine(rootPath, id, "revit_order_new.xml");
                        string savePath = Path.Combine(rootPath, id, "revit_result.xml");
                        //调用dll
                        PythonOrderXml2RevitResultXml.MainProgram d2o = new PythonOrderXml2RevitResultXml.MainProgram(new String[] { readPath, savePath });

                        //完成,执行返回命令
                        Console.WriteLine("执行成功!");
                        //命令
                        string new_cmdStr = $"update {tableName} set `step`=4,`status`=1,`update_time`=\"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}\" where `task_id`={id};";
                        myCmd = new MySqlCommand(new_cmdStr, myConnnect);

                        //执行并关闭
                        mysqldr = myCmd.ExecuteReader();
                        mysqldr.Close();
                    }
                    catch (Exception e)
                    {
                        //失败,执行返回命令
                        Console.WriteLine("执行异常:" + e.ToString());
                        //命令
                        string new_cmdStr = $"update {tableName} set `status`=2,`progress_info`=\"{e.Message}\",`update_time`=\"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}\" where `task_id`={id};";
                        myCmd = new MySqlCommand(new_cmdStr, myConnnect);

                        //执行并关闭
                        mysqldr = myCmd.ExecuteReader();
                        mysqldr.Close();
                    }
                }
                else
                    mysqldr.Close();
            }
            //结束 never gonna say goodbye
            //myConnnect.Close();
        }


    }
}
