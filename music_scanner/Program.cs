using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace music_scanner
{
    class Program
    {
        static DateTime dDateTimeNow = System.DateTime.Now;

        static string sConnectionLog = @"Data Source=" + Environment.MachineName + @";Initial Catalog=SSISLog;User Id=aoostmei;password=Maldor0r;MultipleActiveResultSets=true;Encrypt=True;TrustServerCertificate=True;";
        //// Non Default Connection
        //static string sConnectionLog = @"Data Source=HP840G8;Initial Catalog=SSISLog;User Id=aoostmei;password=Maldor0r;MultipleActiveResultSets=true;Encrypt=True;TrustServerCertificate=True;";
        static string sLogtabel = @"log_exception_table";
        static string sModulenaam = @"music_scan";

        static void Main(string[] args)
        {
            //// Default Connection
            string[] aConnection_env_var = EnvironmentConnection.envconnection(Environment.MachineName, sConnectionLog, sLogtabel, sModulenaam);

            //// Non Default Connection
            //string[] aConnection_env_var = EnvConnectionVar.envconnectionvar("HP840G8");

            try
            {
                LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_main");

                string sConnection = aConnection_env_var[0];
                string sServer = aConnection_env_var[1];
                //string sDatabase = aConnection_env_var[2];
                //string sMachine = aConnection_env_var[3];

                string sConnectionStaging = @"Data Source=" + sServer + @";Initial Catalog=staging;User Id=aoostmei;password=Maldor0r;MultipleActiveResultSets=true;Encrypt=True;TrustServerCertificate=True;";

                //// CONFIGURATION PROCESSING ELEMENTS: 0 = OFF WHEN TOGGLE
                var lConfig = new List<KeyValuePair<int, string>>();
                //// TOGGLE
                /// SCAN FILL VOLUMIO
                lConfig.Add(new KeyValuePair<int, string>(1, @"lossless scan nas source and fill database"));
                lConfig.Add(new KeyValuePair<int, string>(1, @"lossless create volumio playlists based on random view"));
                lConfig.Add(new KeyValuePair<int, string>(1, @"mp3 scan nas source and fill database"));
                lConfig.Add(new KeyValuePair<int, string>(1, @"mp3 create volumio playlists based on random view"));
                //// COPY PLAYLIST ARTIS ALBUM
                lConfig.Add(new KeyValuePair<int, string>(1, @"delete current music selection"));
                lConfig.Add(new KeyValuePair<int, string>(1, @"music selector for album = 1 OR artist = 2"));
                lConfig.Add(new KeyValuePair<int, string>(1, @"lossless copy music for sd card and create m3u playlist"));
                lConfig.Add(new KeyValuePair<int, string>(2, @"lossless create playlist xduoo x2s = 1 or android = 2"));
                //// VALUES
                lConfig.Add(new KeyValuePair<int, string>(1, @"/home/aoostmei/Music/lossless"));
                lConfig.Add(new KeyValuePair<int, string>(1, @"/home/aoostmei/Music/music_selection"));
                lConfig.Add(new KeyValuePair<int, string>(3, @"lossless music selector count albums"));
                lConfig.Add(new KeyValuePair<int, string>(3, @"lossless music selector count artists"));

                //// VARIABLES
                string sRootPath = string.Empty;
                string sSearch = string.Empty;
                string sExtension = string.Empty;
                string sTablename = string.Empty;
                string sSchema = string.Empty;
                string sViewnamePopRandom = string.Empty;
                string sViewnameJazzRandom = string.Empty;
                string sViewnameClassicalRandom = string.Empty;

                string sSourcePath = string.Empty;
                string sTargetPath = string.Empty;
                bool bRecursive;

                sSourcePath = lConfig[8].Value;
                sTargetPath = lConfig[9].Value;
                bRecursive = true;
                string[] sSearchFolders;
                int iCounter = 0;

                if (lConfig.FirstOrDefault(x => x.Value == @"lossless scan nas source and fill database").Key == 1)
                {
                    //// LOSSLESS
                    /////// CREATE SQL OBJECTS FOR FLAC
                    sTablename = @"lossless";
                    sSchema = @"dbo";
                    sSearch = @"*";
                    sRootPath = @"/home/aoostmei/Music/lossless";
                    sExtension = @"flac";
                    sViewnamePopRandom = @"lossless_non_classical_non_jazz_random";
                    sViewnameJazzRandom = @"lossless_jazz_random";
                    sViewnameClassicalRandom = @"lossless_classical_random";

                    //// CREATE SQL OBJECTS FOR LOSSLESS
                    CreateSQLObjects.createsqlobjects(sConnectionLog, sLogtabel, sModulenaam, sTablename, sSchema, sConnectionStaging);

                    //// CREATE DATATABLE FOR LOSSLESS
                    DataTable dtMusicTableLossless = FillDatatable.filldatatable(sConnectionLog, sLogtabel, sModulenaam, sRootPath, sSearch, sExtension, dDateTimeNow);

                    //// INSERT SQL DATA FOR LOSSLESS
                    SqlBulkcopy.sqlbulkcopy(sConnectionLog, sLogtabel, sModulenaam, sTablename, sSchema, sConnectionStaging, dtMusicTableLossless);

                }
                if (lConfig.FirstOrDefault(x => x.Value == @"lossless create volumio playlists based on random view").Key == 1)
                {
                    //// CREATE VOLUMIO PLAYLISTS FOR LOSSLESS
                    CreateVolumioPlaylists.createvolumioplaylists(sConnectionLog, sLogtabel, sModulenaam, sSchema, sViewnamePopRandom, sConnectionStaging);
                    CreateVolumioPlaylists.createvolumioplaylists(sConnectionLog, sLogtabel, sModulenaam, sSchema, sViewnameJazzRandom, sConnectionStaging);
                    CreateVolumioPlaylists.createvolumioplaylists(sConnectionLog, sLogtabel, sModulenaam, sSchema, sViewnameClassicalRandom, sConnectionStaging);
                }

                if (lConfig.FirstOrDefault(x => x.Value == @"mp3 scan nas source and fill database").Key == 1)
                {
                    //// MP3
                    //// CREATE SQL OBJECTS FOR MP3
                    sTablename = @"mp3";
                    sSchema = @"dbo";
                    sRootPath = @"/home/aoostmei/Music/mp3";
                    sExtension = @"mp3";
                    sTablename = @"mp3";
                    sViewnamePopRandom = @"mp3_non_classical_non_jazz_random";
                    sViewnameJazzRandom = @"mp3_jazz_random";
                    sViewnameClassicalRandom = @"mp3_classical_random";

                    //// CREATE SQL OBJECTS FOR MP3
                    CreateSQLObjects.createsqlobjects(sConnectionLog, sLogtabel, sModulenaam, sTablename, sSchema, sConnectionStaging);

                    //// CREATE DATATABLE FOR MP3
                    DataTable dtMusicTableMp3 = FillDatatable.filldatatable(sConnectionLog, sLogtabel, sModulenaam, sRootPath, sSearch, sExtension, dDateTimeNow);

                    //// INSERT SQL DATA FOR MP3
                    SqlBulkcopy.sqlbulkcopy(sConnectionLog, sLogtabel, sModulenaam, sTablename, sSchema, sConnectionStaging, dtMusicTableMp3);
                }
                if (lConfig.FirstOrDefault(x => x.Value == @"mp3 create volumio playlists based on random view").Key == 1)
                {
                    //// CREATE VOLUMIO PLAYLISTS FOR MP3
                    CreateVolumioPlaylists.createvolumioplaylists(sConnectionLog, sLogtabel, sModulenaam, sSchema, sViewnamePopRandom, sConnectionStaging);
                    CreateVolumioPlaylists.createvolumioplaylists(sConnectionLog, sLogtabel, sModulenaam, sSchema, sViewnameJazzRandom, sConnectionStaging);
                    CreateVolumioPlaylists.createvolumioplaylists(sConnectionLog, sLogtabel, sModulenaam, sSchema, sViewnameClassicalRandom, sConnectionStaging);
                }

                //// COPY MUSIC FOLDERS FOR SD CARD
                if (lConfig.FirstOrDefault(x => x.Value == @"delete current music selection").Key == 1)
                {
                    //// DELETE TARGET MUSIC FOLDER CONTENT
                    DeleteFolderContent.deletefoldercontent(sConnectionLog, sLogtabel, sModulenaam, sTargetPath);
                }
                //// CREATE MUSIC SELECTION STRING BY ALBUM
                if (lConfig.FirstOrDefault(x => x.Value == @"music selector for album = 1 OR artist = 2").Key == 1 || lConfig.FirstOrDefault(x => x.Value == @"music selector for album = 1 OR artist = 2").Key == 2)
                {
                    if (lConfig.FirstOrDefault(x => x.Value == @"music selector for album = 1 OR artist = 2").Key == 1)
                    {
                        //// BY ALBUM 60 > iType = 1 OR ARTIST 12 > iType = 2
                        sSearchFolders = MusicSelector.musicselector(sConnectionLog, sLogtabel, sModulenaam, sConnectionStaging, lConfig[5].Key, lConfig[10].Key).ToArray();

                        StreamWriter writer = new StreamWriter(sTargetPath + @"/music_selection.m3u", true);

                        writer.WriteLine(@"#EXTM3U");

                        CopyProcessing.copyprocessing(sConnectionLog, sLogtabel, sModulenaam, sSourcePath, sTargetPath, bRecursive, sSearchFolders, writer, iCounter, lConfig[7].Key);

                        writer.Close();
                    }
                    if (lConfig.FirstOrDefault(x => x.Value == @"music selector for album = 1 OR artist = 2").Key == 2)
                    {
                        //// BY ALBUM 60 > iType = 1 OR ARTIST 12 > iType = 2
                        sSearchFolders = MusicSelector.musicselector(sConnectionLog, sLogtabel, sModulenaam, sConnectionStaging, lConfig[5].Key, lConfig[11].Key).ToArray();

                        StreamWriter writer = new StreamWriter(sTargetPath + @"/music_selection.m3u", true);

                        writer.WriteLine(@"#EXTM3U");

                        CopyProcessing.copyprocessing(sConnectionLog, sLogtabel, sModulenaam, sSourcePath, sTargetPath, bRecursive, sSearchFolders, writer, iCounter, lConfig[7].Key);

                        writer.Close();
                    }
                }

                LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_main");

            }
            catch (Exception ex_main)
            {
                LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_main", ex_main.Message.Replace("'", "~"));
            }
            finally
            {
            }
        }

        internal class CreateVolumioPlaylists
        {
            public static void createvolumioplaylists(string sConnectionLog, string sLogtabel, string sModulenaam, string sSchema, string sViewname, string sConnectionStaging)
            {
                string sQuery = string.Empty;
                string sJsonvolumioPlaylist = string.Empty;

                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_createvolumioplaylists");

                    SqlConnection conSource = new SqlConnection(sConnectionStaging);

                    conSource.Open();

                    // Random Pop Lossless
                    sQuery = "SELECT [service] = 'mpd', [uri] = 'mnt' + replace(fullpath,'home/aoostmei/Music','NAS'), [title] = [fullfilename],[artist] = '',[album] = '',[albumart] = '/albumart?cacheid=588&web=////extralarge&path=%2Fmnt%2FNAS%2Flossless%2F'+[artist]+'%2F'+[album]+'&icon=fa-tags&metadata=false' FROM [" + sSchema + "].[" + sViewname + "]";

                    DataTable dtSource = new DataTable();

                    SqlCommand cmdSource = new SqlCommand(sQuery, conSource);
                    SqlDataAdapter daSource = new SqlDataAdapter(cmdSource);

                    using (daSource)
                    {
                        daSource.Fill(dtSource);
                    }

                    conSource.Close();

                    sJsonvolumioPlaylist = DataTableStringBuilder.datatablestringbuilder(sConnectionLog, sLogtabel, sModulenaam, dtSource);

                    using (StreamWriter writer = new StreamWriter(@"/home/aoostmei/Music/volumio_playlists_generated/" + sViewname + ".txt", false))
                    {
                        writer.WriteLine(sJsonvolumioPlaylist);
                    }

                    dtSource.Clear();
                    daSource.Dispose();

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_createvolumioplaylists");
                }
                catch (Exception ex_createvolumioplaylists)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_createvolumioplaylists", ex_createvolumioplaylists.Message.Replace("'", "~"));
                }
                finally
                {
                }
            }
        }

        internal class MusicSelector
        {
            public static List<string> musicselector(string sConnectionLog, string sLogtabel, string sModulenaam, string sConnectionStaging, int iType, int iCountObjects)
            {
                string sQuery = string.Empty;
                string sQueryCombined = string.Empty;
                List<string> lMusicSelector = new List<string>();

                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_musicselector");

                    if (iType == 1)
                    {

                        sQuery = @"SELECT TOP " + iCountObjects + @" [album] FROM [staging].[dbo].[lossless] l WHERE NOT EXISTS (SELECT [composer] FROM [staging].[dbo].[composers] c WHERE l.[artist] = c.[composer] AND c.[type] IN ('classical','jazz')) GROUP BY l.[album] ORDER BY NEWID()";
                    }

                    if (iType == 2)
                    {
                        sQuery = @"SELECT TOP " + iCountObjects + @" [artist] FROM [staging].[dbo].[lossless] l WHERE NOT EXISTS (SELECT [composer] FROM [staging].[dbo].[composers] c WHERE l.[artist] = c.[composer] AND c.[type] IN ('classical','jazz')) GROUP BY l.[artist] ORDER BY NEWID()";
                    }

                    sQueryCombined = string.Format(sQuery);

                    using (SqlConnection conStaging = new SqlConnection(sConnectionStaging))
                    {
                        conStaging.Open();

                        using (SqlCommand command = new SqlCommand(sQueryCombined, conStaging))
                        {

                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read() && reader.HasRows)
                                {
                                    var album = reader["album"]?.ToString();
                                    if (!string.IsNullOrEmpty(album))
                                    {
                                        lMusicSelector.Add(album);
                                    }
                                }
                            }
                        }

                        conStaging.Close();
                    }

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_musicselector");

                }
                catch (Exception ex_musicselector)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_musicselector", ex_musicselector.Message.Replace("'", "~"));
                }
                finally
                {
                }

                return lMusicSelector;
            }
        }

        internal class CopyProcessing
        {
            public static void copyprocessing(string sConnectionLog, string sLogtabel, string sModulenaam, string sSourcePath, string sTargetPath, bool bRecursive, string[] sSearchFolders, StreamWriter writer, int iCounter, int iPLatform)
            {
                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_copyprocessing");

                    //// COPY MUSIC SELECTION TO LOCAL DIRECTORY
                    ///
                    // Get information about the source directory
                    var dir = new DirectoryInfo(sSourcePath);

                    // Check if the source directory exists
                    if (!dir.Exists)
                        throw new DirectoryNotFoundException($"Source directory not found: {dir.FullName}");

                    // Cache directories before we start copying
                    DirectoryInfo[] dirs = dir.GetDirectories();

                    // Create the destination directory
                    Directory.CreateDirectory(sTargetPath);

                    int iFileCounter = 0;
                    // Get the files in the source directory and copy to the destination directory
                    foreach (FileInfo file in dir.GetFiles())
                    {
                        iFileCounter++;

                        string targetFilePath = Path.Combine(sTargetPath, file.Name);
                        file.CopyTo(targetFilePath, false);

                        writer.WriteLine(@"#EXTINF:" + Math.Abs((iCounter - dir.GetFiles().Count() + iFileCounter)).ToString() + "," + file.Name);
                        //// XDUOO X2S
                        if (iPLatform == 1)
                        {
                            writer.WriteLine(file.FullName);
                        }
                        //// ANDROID
                        if (iPLatform == 2)
                        {
                            writer.WriteLine(file.FullName);
                        }
                    }

                    // If recursive and copying subdirectories, recursively call this method
                    if (bRecursive)
                    {
                        foreach (DirectoryInfo subDir in dirs)
                        {
                            foreach (string sSearchFolder in sSearchFolders)
                            {
                                foreach (DirectoryInfo subsubDir in subDir.GetDirectories())
                                {
                                    if ((subDir.FullName + @"/").Contains(@"/" + sSearchFolder + @"/"))
                                    {
                                        iCounter = iCounter + subsubDir.GetFiles().Count();

                                        string newDestinationDir = Path.Combine(sTargetPath, subDir.Name);
                                        CopyProcessing.copyprocessing(sConnectionLog, sLogtabel, sModulenaam, subDir.FullName, newDestinationDir, true, sSearchFolders, writer, iCounter, iPLatform);
                                    }
                                    if ((subsubDir.FullName + @"/").Contains(@"/" + sSearchFolder + @"/"))
                                    {
                                        iCounter = iCounter + subsubDir.GetFiles().Count();

                                        string newDestinationDir = Path.Combine(sTargetPath, subDir.Name, subsubDir.Name);
                                        CopyProcessing.copyprocessing(sConnectionLog, sLogtabel, sModulenaam, subsubDir.FullName, newDestinationDir, true, sSearchFolders, writer, iCounter, iPLatform);
                                    }
                                }
                            }
                        }
                    }

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_copyprocessing");
                }
                catch (Exception ex_copyprocessing)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_copyprocessing", ex_copyprocessing.Message.Replace("'", "~"));
                }
                finally
                {
                }
            }
        }

        internal class DeleteFolderContent
        {
            public static void deletefoldercontent(string sConnectionLog, string sLogtabel, string sModulenaam, string sTargetPath)
            {
                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_deletefoldercontent");

                    //// Get information about the source directory
                    var dir = new DirectoryInfo(sTargetPath);

                    //// DELETE ALL
                    foreach (FileInfo file in dir.GetFiles())
                    {
                        file.Delete();
                    }
                    foreach (DirectoryInfo di in dir.GetDirectories())
                    {
                        di.Delete(true);
                    }

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_deletefoldercontent");
                }


                catch (Exception ex_deletefoldercontent)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_deletefoldercontent", ex_deletefoldercontent.Message.Replace("'", "~"));
                }
                finally
                {
                }
            }
        }

        internal class DataTableStringBuilder
        {
            public static string datatablestringbuilder(string sConnectionLog, string sLogtabel, string sModulenaam, DataTable dataTable)
            {
                var jsonStringBuilder = new StringBuilder();

                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_datatablestringbuilder");

                    if (dataTable == null)
                    {
                        return string.Empty;
                    }

                    if (dataTable.Rows.Count > 0)
                    {
                        jsonStringBuilder.Append("[");
                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            jsonStringBuilder.Append("{");
                            for (int j = 0; j < dataTable.Columns.Count; j++)
                                jsonStringBuilder.AppendFormat("\"{0}\":\"{1}\"{2}",
                                        dataTable.Columns[j].ColumnName.ToString(),
                                        //dataTable.Rows[i][j].ToString(),

                                        //// Volumio Specific
                                        dataTable.Rows[i][j]?.ToString() ?? "".Replace("\\", "/").Replace("//", "/").Replace("/music/", "/"),
                                        j < dataTable.Columns.Count - 1 ? "," : string.Empty);
                            jsonStringBuilder.Append(i == dataTable.Rows.Count - 1 ? "}" : "},");
                        }
                        jsonStringBuilder.Append("]");
                    }

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_datatablestringbuilder");
                }
                catch (Exception ex_datatablestringbuilder)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_datatablestringbuilder", ex_datatablestringbuilder.Message.Replace("'", "~"));
                }
                finally
                {
                }

                return jsonStringBuilder.ToString();
            }
        }

        internal class SqlBulkCopyHelper
        {
            static FieldInfo? rowsCopiedField = null;

            /// <summary>
            /// Gets the rows copied from the specified SqlBulkCopy object
            /// </summary>
            /// <param name="bulkCopy">The bulk copy.</param>
            /// <returns></returns>
            public static int GetRowsCopied(string sConnectionLog, string sLogtabel, string sModulenaam, SqlBulkCopy bulkCopy)
            {
                if (rowsCopiedField == null)
                {
                    rowsCopiedField = typeof(SqlBulkCopy).GetField("_rowsCopied", BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Instance);
                }

                if (rowsCopiedField == null)
                {
                    return 0;
                }
                else
                {
                    object? value = rowsCopiedField.GetValue(bulkCopy);
                    return value != null ? (int)(Int64)value : 0;
                    //return (int)(Int64)rowsCopiedField.GetValue(bulkCopy);
                }
            }
        }

        internal class SqlBulkcopy
        {
            public static void sqlbulkcopy(string sConnectionLog, string sLogtabel, string sModulenaam, string sTablename, string sSchema, string sConnectionStaging, DataTable dtMusicTable)
            {
                int iBron = 0;

                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_sqlbulkcopy");

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sConnectionStaging))
                    {
                        bulkCopy.DestinationTableName = "[" + sSchema + "].[" + sTablename + "]";

                        try
                        {
                            bulkCopy.WriteToServer(dtMusicTable);

                            iBron = SqlBulkCopyHelper.GetRowsCopied(sConnectionLog, sLogtabel, sModulenaam, bulkCopy);

                            LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, sTablename, " ROWCOUNT VALUE: " + iBron.ToString());

                        }
                        catch (Exception ex_sqlbulkcopy_inner)
                        {
                            LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_sqlbulkcopy_inner", ex_sqlbulkcopy_inner.Message.Replace("'", "~"));
                        }
                        finally
                        {
                            dtMusicTable.Clear();
                        }
                    }

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_sqlbulkcopy");
                }
                catch (Exception ex_sqlbulkcopy)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_sqlbulkcopy", ex_sqlbulkcopy.Message.Replace("'", "~"));
                }
                finally
                {
                }
            }
        }

        internal class FillDatatable
        {
            public static DataTable filldatatable(string sConnectionLog, string sLogtabel, string sModulenaam, string sRootPath, string sSearch, string sExtension, DateTime dDateTimeNow)
            {
                string sCounter = string.Empty;
                string sFullPath = string.Empty;
                string? sFullFileName = string.Empty;
                string? sArtist = string.Empty;
                string? sAlbum = string.Empty;
                string? sTrack = string.Empty;

                DataTable dtMusicTable = new DataTable();

                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_fill_datatable");

                    DataColumn dcCounter = dtMusicTable.Columns.Add("counter", typeof(Int32));
                    dcCounter.AllowDBNull = false;

                    DataColumn dcFullpath = dtMusicTable.Columns.Add("fullpath", typeof(string));
                    dcFullpath.AllowDBNull = true;

                    DataColumn dcFullfilename = dtMusicTable.Columns.Add("fullfilename", typeof(string));
                    dcFullfilename.AllowDBNull = true;

                    DataColumn dcArtist = dtMusicTable.Columns.Add("artist", typeof(string));
                    dcArtist.AllowDBNull = true;

                    DataColumn dcAlbum = dtMusicTable.Columns.Add("album", typeof(string));
                    dcAlbum.AllowDBNull = true;

                    DataColumn dcTrack = dtMusicTable.Columns.Add("track", typeof(string));
                    dcTrack.AllowDBNull = true;

                    DataColumn dcExtraction_date = dtMusicTable.Columns.Add("extraction_date", typeof(DateTime));
                    dcExtraction_date.AllowDBNull = false;

                    var fullpath = Directory
                        .EnumerateFiles(sRootPath, "*" + sSearch + "*." + sExtension + "", SearchOption.AllDirectories)
                        .ToArray();

                    if (sRootPath.StartsWith(@"/"))
                    {
                        for (int i = 0; i < fullpath.Count(); i++)
                        {
                            sCounter = i.ToString();
                            sFullPath = fullpath[i].ToString();
                            sFullFileName = fullpath[i].ToString().Split('/').Count() >= 8 ? fullpath[i].ToString().Split('/')[7] : null;
                            sArtist = fullpath[i].ToString().Split('/').Count() >= 6 ? fullpath[i].ToString().Split('/')[5] : null;
                            sAlbum = fullpath[i].ToString().Split('/').Count() >= 7 ? fullpath[i].ToString().Split('/')[6] : null;
                            sTrack = fullpath[i].ToString().Split('/').Count() >= 8 ? fullpath[i].ToString().Split('/')[7].Replace("." + sExtension, string.Empty) : null;

                            dtMusicTable.Rows.Add(sCounter, sFullPath, sFullFileName, sArtist, sAlbum, sTrack, dDateTimeNow);
                        }
                    }

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_fill_datatable");
                }
                catch (Exception ex_filldatatable)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_filldatatable", ex_filldatatable.Message.Replace("'", "~"));
                }
                finally
                {
                }

                return dtMusicTable;
            }
        }

        internal class CreateSQLObjects
        {
            public static void createsqlobjects(string sConnectionLog, string sLogtabel, string sModulenaam, string sTablename, string sSchema, string sConnectionStaging)
            {
                string sQuery = string.Empty;
                string sQueryCombined = string.Empty;

                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_create_sql_objects");

                    sQuery = @"IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{0}].[{1}]') AND type in (N'U'))
                        BEGIN
                        DROP TABLE [{0}].[{1}]
                        END";

                    sQueryCombined = string.Format(sQuery, sSchema, sTablename);
                    ExecuteSql.executesql("drop table", sConnectionLog, sLogtabel, sModulenaam, sConnectionStaging, sQueryCombined);

                    sQuery = @"IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{0}].[{1}]') AND type in (N'U'))
                        BEGIN
                        CREATE TABLE [{0}].[{1}](
                         [counter] [int] IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,
                         [fullpath] [nvarchar](2000) NULL,
                         [fullfilename] [nvarchar](2000) NULL,
                         [artist] [nvarchar](2000) NULL,
                         [album] [nvarchar](2000) NULL,
                         [track] [nvarchar](2000) NULL,
                            [extraction_date] [datetime] NOT NULL
                        ) -- WITH (MEMORY_OPTIMIZED=ON) 
                        END";

                    sQueryCombined = string.Format(sQuery, sSchema, sTablename);
                    ExecuteSql.executesql("create table", sConnectionLog, sLogtabel, sModulenaam, sConnectionStaging, sQueryCombined);

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_create_sql_objects");
                }
                catch (Exception ex_createsqlobjects)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_createsqlobjects", ex_createsqlobjects.Message.Replace("'", "~"));
                }
                finally
                {
                }
            }
        }

        internal class ExecuteSql
        {
            public static void executesql(string sTaskInfo, string sConnectionLog, string sLogtabel, string sModulenaam, string sConnection, string sQuery)
            {

                SqlConnection connection = new SqlConnection();
                connection.ConnectionString = sConnection;

                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_executesql");

                    connection.Open();

                    SqlCommand cmd_execute = new SqlCommand(sQuery, connection);
                    cmd_execute.CommandTimeout = 30000;
                    cmd_execute.ExecuteNonQuery();

                    connection.Close();

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_executesql");

                }
                catch (Exception ex_executesql)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam + " " + sTaskInfo, "ex_executesql", sQuery.Replace("'", "~") + " Message >> " + ex_executesql.Message.Replace("'", "~"));
                }
                finally
                {
                }
            }
        }

        internal class LogWrite
        {
            public static void logwrite(string sConnectionLog, string sLogtabel, string sModulenaam, string sModulefase, string sMessage)
            {
                string sQueryLog = @"INSERT INTO [dbo].[" + sLogtabel + "] SELECT GETDATE(), '" + sModulenaam + "', '" + sModulefase + "','" + sMessage + "'";

                SqlConnection connection_log = new SqlConnection();
                connection_log.ConnectionString = sConnectionLog;

                try
                {
                    connection_log.Open();

                    SqlCommand cmd_execute = new SqlCommand(sQueryLog, connection_log);
                    cmd_execute.CommandTimeout = 30000;
                    cmd_execute.ExecuteNonQuery();

                    connection_log.Close();
                }
                catch (Exception ex_logwrite)
                {
                    Console.WriteLine("Error LogWrite: " + ex_logwrite.Message.Replace("'", "~"));
                }
                finally
                {
                }
            }
        }

        internal class EnvironmentConnection
        {
            public static string[] envconnection(string sMachine, string sConnectionLog, string sLogtabel, string sModulenaam)
            {
                string[] aConnectionEnvVar = { "", "", "", "" };

                try
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "START", "start_envconnection");

                    if (sMachine.Length > 0)
                    {
                        aConnectionEnvVar[0] = @"Data Source=" + sMachine + @";Initial Catalog=SSISConfig;User Id=aoostmei;password=Maldor0r;MultipleActiveResultSets=true;Encrypt=True;TrustServerCertificate=True;";
                        aConnectionEnvVar[1] = sMachine;
                        aConnectionEnvVar[2] = @"SSISConfig";
                        aConnectionEnvVar[3] = sMachine;
                    }

                    try
                    {
                        SqlConnection cConTest = new SqlConnection { ConnectionString = aConnectionEnvVar[0] };
                        cConTest.Open();
                        cConTest.Close();
                    }
                    catch (Exception ex_connectiontest)
                    {
                        LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_connectiontest", ex_connectiontest.Message.Replace("'", "~"));
                    }

                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "END", "end_envconnection");
                }
                catch (Exception ex_envconnection)
                {
                    LogWrite.logwrite(sConnectionLog, sLogtabel, sModulenaam, "ex_envconnection", ex_envconnection.Message.Replace("'", "~"));
                }

                return aConnectionEnvVar;
            }
        }
    }
}