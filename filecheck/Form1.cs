using System;
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;

namespace filecheck
{
    public partial class Form1 : Form
    {
        private MariaDB mydb = new MariaDB("localhost", "3306", "root", "25800478*", "filecheck");

        public Form1()
        {
            InitializeComponent();
            connDB();
        }

        private void connDB()
        {
            if (mydb.GetDBConnectTest() == true)
            {
                Console.WriteLine("db connect");
            }
            else
            {
                Console.WriteLine("db is not connected");
            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            if (folderBrowserDialog1.ShowDialog() == DialogResult.OK)
            {
                textBox1.Text = folderBrowserDialog1.SelectedPath;
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            Stopwatch stopwatch = new Stopwatch();

            Console.Write("Ins Start : ");
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.ffff"));
            string sFilepath;
            string sFilename;
            string sWdate, sWtime;
            string sQry;

            mydb.DBConnect();

            stopwatch.Start();

            System.IO.DirectoryInfo fi = new System.IO.DirectoryInfo(textBox1.Text);

            foreach (var filename in fi.GetFiles("*.*", SearchOption.AllDirectories))
            {
                sFilepath = filename.FullName;
                sFilename = filename.Name;
                sWdate = DateTime.Now.ToString("yyyy-MM-dd");
                sWtime = DateTime.Now.ToString("hh:mm:ss.ffff");
                //sQry = string.Format("insert into FILELIST (FILEPATH, FILENAME, WDATE, WTIME) VALUES ('{0}', '{1}', '{2}', '{3}');", sFilepath, sFilename, sWdate, sWtime);
                sQry = string.Format("insert into FILELIST (FILEPATH, FILENAME, WDATE, WTIME)" +
                    " SELECT '{0}', '{1}', '{2}', '{3}' " +
                    " FROM DUAL" +
                    " WHERE NOT EXISTS (SELECT 1 FROM FILELIST WHERE " +
                    " FILEPATH='{0}' AND FILENAME='{1}' AND WDATE='{2}');", sFilepath, sFilename, sWdate, sWtime);

                mydb.ExecuteQuery(sQry);
            }


            mydb.DBDisconnect();
            
            Console.Write("Ins End : ");
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.ffff"));
            stopwatch.Stop();
            Console.WriteLine("Elapsed Time : {0}", stopwatch.Elapsed);
        }
    }
}