using System.Text;
using FirebirdSql.Data.FirebirdClient;

namespace DbMetaTool
{
    public static class Program
    {
        private const string DomainScriptsSubdir = "domains";
        private const string TableScriptsSubdir = "tables";
        private const string ProcedureScriptsSubdir = "procedures";

        // Przykładowe wywołania:
        // DbMetaTool build-db --db-dir "C:\db\fb5" --scripts-dir "C:\scripts"
        // DbMetaTool export-scripts --connection-string "..." --output-dir "C:\out"
        // DbMetaTool update-db --connection-string "..." --scripts-dir "C:\scripts"
        public static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Użycie:");
                Console.WriteLine("  build-db --db-dir <ścieżka> --scripts-dir <ścieżka>");
                Console.WriteLine("  export-scripts --connection-string <connStr> --output-dir <ścieżka>");
                Console.WriteLine("  update-db --connection-string <connStr> --scripts-dir <ścieżka>");
                return 1;
            }

            try
            {
                var command = args[0].ToLowerInvariant();

                switch (command)
                {
                    case "build-db":
                    {
                        string dbDir = GetArgValue(args, "--db-dir");
                        string scriptsDir = GetArgValue(args, "--scripts-dir");

                        BuildDatabase(dbDir, scriptsDir);
                        Console.WriteLine("Baza danych została zbudowana pomyślnie.");
                        return 0;
                    }

                    case "export-scripts":
                    {
                        string connStr = GetArgValue(args, "--connection-string");
                        string outputDir = GetArgValue(args, "--output-dir");

                        ExportScripts(connStr, outputDir);
                        Console.WriteLine("Skrypty zostały wyeksportowane pomyślnie.");
                        return 0;
                    }

                    case "update-db":
                    {
                        string connStr = GetArgValue(args, "--connection-string");
                        string scriptsDir = GetArgValue(args, "--scripts-dir");

                        UpdateDatabase(connStr, scriptsDir);
                        Console.WriteLine("Baza danych została zaktualizowana pomyślnie.");
                        return 0;
                    }

                    default:
                        Console.WriteLine($"Nieznane polecenie: {command}");
                        return 1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd: " + ex.Message);
                return -1;
            }
        }

        private static string GetArgValue(string[] args, string name)
        {
            int idx = Array.IndexOf(args, name);
            if (idx == -1 || idx + 1 >= args.Length)
                throw new ArgumentException($"Brak wymaganego parametru {name}");
            return args[idx + 1];
        }

        #region Budowanie ze skryptów

        /// <summary>
        /// Buduje nową bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void BuildDatabase(string databaseDirectory, string scriptsDirectory)
        {
            if (string.IsNullOrWhiteSpace(databaseDirectory))
                throw new ArgumentException("Ścieżka bazy danych musi być podana.", nameof(databaseDirectory));
            if (string.IsNullOrWhiteSpace(scriptsDirectory))
                throw new ArgumentException("Ścieżka folderu ze skryptami musi być podana", nameof(scriptsDirectory));
            if (!Directory.Exists(scriptsDirectory))
                throw new DirectoryNotFoundException($"Nie znaleziono folderu ze skryptami: {scriptsDirectory}");

            Directory.CreateDirectory(databaseDirectory);

            var dbPath = Path.Combine(databaseDirectory, "database.fdb");

            if (File.Exists(dbPath))
            {
                File.Delete(dbPath);
                Console.WriteLine($"Usuwanie istniejącej bazy: {dbPath}");
            }

            string conn =
                "User=SYSDBA;" +
                "Password=haslo1234;" +
                $"Database={dbPath};" +
                "DataSource=localhost;" +
                "Port=3050;" +
                "Dialect=3;" +
                "Charset=UTF8;" +
                "Pooling=true;";

            FbConnection.CreateDatabase(conn);

            using var connection = new FbConnection(conn);
            connection.Open();

            var failed = new List<string>();
            int executed = 0;

            // 1) Domains
            var domainsFolder = Path.Combine(scriptsDirectory, DomainScriptsSubdir);
            var r1 = RunFolderSqlFiles(connection, domainsFolder, BuildDomainsFromFile);
            executed += r1.Executed;
            failed.AddRange(r1.Failed);

            // 2) Tables
            var tablesFolder = Path.Combine(scriptsDirectory, TableScriptsSubdir);
            var r2 = RunFolderSqlFiles(connection, tablesFolder,ExecuteSqlFile) ;
            executed += r2.Executed;
            failed.AddRange(r2.Failed);

            // 3) Procedures
            var procsFolder = Path.Combine(scriptsDirectory, ProcedureScriptsSubdir);
            var r3 = RunFolderSqlFiles(connection, procsFolder, ExecuteSqlFile);
            executed += r3.Executed;
            failed.AddRange(r3.Failed);
            connection.Close();
           
            if (failed.Count > 0)
            {
                Console.WriteLine("Niektóre pliki nie załadowały się poprawnie:");
                foreach (var s in failed) Console.WriteLine($" - {s}");
                throw new Exception("Jeden bądź więcej skryptów nie załadowało się poprawnie.");
            }
            
            Console.WriteLine($"Skrypty załadowane poprawnie: {executed}");
        }

        #endregion

        #region Eksport skryptów

        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public static void ExportScripts(string connectionString, string outputDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Pobierz metadane domen, tabel (z kolumnami) i procedur.
            // 3) Wygeneruj pliki .sql / .json / .txt w outputDirectory.

            using var connection = new FbConnection(connectionString);
            connection.Open();
            ExportDomains(connection, outputDirectory);
            ExportTables(connection, outputDirectory);
            ExportProcedures(connection, outputDirectory);
            connection.Close();
        }

        private static void ExportDomains(FbConnection connection, string outputDirectory)
        {
            string sql = @"
            SELECT 
                TRIM(RDB$FIELD_NAME) AS DOMAIN_NAME,
                RDB$FIELD_TYPE,
                RDB$FIELD_SUB_TYPE,
                RDB$FIELD_LENGTH,
                RDB$CHARACTER_LENGTH,
                RDB$FIELD_SCALE,
                RDB$CHARACTER_SET_ID,
                RDB$DEFAULT_SOURCE,
                RDB$VALIDATION_SOURCE,
                RDB$NULL_FLAG
            FROM RDB$FIELDS
            WHERE RDB$SYSTEM_FLAG = 0
            ORDER BY RDB$FIELD_NAME";

            var domainsDir = Path.Combine(outputDirectory, DomainScriptsSubdir);
            Directory.CreateDirectory(domainsDir);

            var combined = new StringBuilder();

            using (var cmd = new FbCommand(sql, connection))
            using (var r = cmd.ExecuteReader())
            {
                while (r.Read())
                {
                    string? name = r["DOMAIN_NAME"] as string;
                    if (name is null)
                        throw new Exception("Nazwa domeny jest null.");

                    if (name.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase))
                        continue;

                    int fieldType = Convert.ToInt32(r["RDB$FIELD_TYPE"]);
                    int length = Convert.ToInt32(r["RDB$FIELD_LENGTH"]);
                    int charLength = r["RDB$CHARACTER_LENGTH"] is DBNull
                        ? -1
                        : Convert.ToInt32(r["RDB$CHARACTER_LENGTH"]);
                    int scale = Convert.ToInt32(r["RDB$FIELD_SCALE"]);
                    int charsetId = r["RDB$CHARACTER_SET_ID"] is DBNull
                        ? -1
                        : Convert.ToInt32(r["RDB$CHARACTER_SET_ID"]);

                    string? defaultSrc = r["RDB$DEFAULT_SOURCE"] as string;
                    string? checkSrc = r["RDB$VALIDATION_SOURCE"] as string;
                    bool notNull = r["RDB$NULL_FLAG"] != DBNull.Value;

                    string? dataType = GetDataType(fieldType, length, charLength, scale);

                    var sb = new StringBuilder();
                    sb.Append($"CREATE DOMAIN {name} AS {dataType}");

                    if (charsetId > 0)
                        sb.Append($"  CHARACTER SET {GetCharsetName(charsetId)}");

                    if (!string.IsNullOrWhiteSpace(defaultSrc))
                        sb.Append($"  {defaultSrc.Trim()}");

                    if (!string.IsNullOrWhiteSpace(checkSrc))
                        sb.Append($"  {checkSrc.Trim()}");

                    if (notNull)
                        sb.Append("  NOT NULL");

                    sb.Append(';');
                    sb.AppendLine();

                    // write individual domain file
                    var fileName = $"domain_{SanitizeFileName(name)}.sql";
                    var filePath = Path.Combine(domainsDir, fileName);
                    File.WriteAllText(filePath, sb.ToString(), Encoding.UTF8);

                    // append to combined
                    combined.Append(sb);
                }
            }
        }


        private static string? GetDataType(int fieldType, int length, int charLength, int scale)
        {
            return fieldType switch
            {
                7 => scale < 0 ? $"NUMERIC({length}, {-scale})" : "SMALLINT",
                8 => scale < 0 ? $"NUMERIC({length}, {-scale})" : "INTEGER",
                10 => "FLOAT",
                12 => "DATE",
                13 => "TIME",
                14 => $"CHAR({charLength})",
                16 => scale < 0 ? $"DECIMAL({length}, {-scale})" : "BIGINT",
                27 => "DOUBLE PRECISION",
                35 => "TIMESTAMP",
                37 => $"VARCHAR({charLength})",
                40 => "CSTRING",
                _ => "BLOB"
            };
        }

        private static string GetCharsetName(int id)
        {
            return id switch
            {
                0 => "NONE",
                1 => "ASCII",
                2 => "BIG_5",
                3 => "CYRL",
                4 => "DOS437",
                5 => "DOS850",
                6 => "DOS865",
                8 => "ISO8859_1",
                21 => "UTF8",
                _ => "NONE"
            };
        }

        private static void ExportTables(FbConnection connection, string outputDirectory)
        {
            string sqlTables = @"
            SELECT TRIM(RDB$RELATION_NAME) AS TABLE_NAME
            FROM RDB$RELATIONS
            WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_SOURCE IS NULL";

            var tablesDir = Path.Combine(outputDirectory, TableScriptsSubdir);
            Directory.CreateDirectory(tablesDir);

            var combined = new StringBuilder();

            using var cmdTables = new FbCommand(sqlTables, connection);
            using var rt = cmdTables.ExecuteReader();

            while (rt.Read())
            {
                var table = rt["TABLE_NAME"].ToString()?.Trim();

                if (string.IsNullOrWhiteSpace(table))
                    throw new Exception("Nieobsługiwany przypadek: nazwa tabeli jest null lub pusta.");

                var tableSql = ExportTableDefinition(connection, table);
                var fileName = $"{SanitizeFileName(table)}.sql";
                var filePath = Path.Combine(tablesDir, fileName);
                File.WriteAllText(filePath, tableSql, Encoding.UTF8);

                combined.AppendLine(tableSql);
            }
        }

        private static string ExportTableDefinition(FbConnection connection, string table)
        {
            string sqlFields = @"
            SELECT 
                TRIM(RF.RDB$FIELD_NAME) AS FIELD_NAME,
                TRIM(F.RDB$FIELD_NAME) AS DOMAIN_NAME,
                F.RDB$FIELD_TYPE,
                F.RDB$FIELD_SUB_TYPE,
                F.RDB$FIELD_LENGTH,
                F.RDB$CHARACTER_LENGTH,
                F.RDB$FIELD_SCALE,
                F.RDB$CHARACTER_SET_ID,
                RF.RDB$DEFAULT_SOURCE,
                F.RDB$VALIDATION_SOURCE,
                RF.RDB$NULL_FLAG,
                RF.RDB$DESCRIPTION
            FROM RDB$RELATION_FIELDS RF
            JOIN RDB$FIELDS F ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE
            WHERE RF.RDB$RELATION_NAME = @T
            ORDER BY RF.RDB$FIELD_POSITION";

            using var cmd = new FbCommand(sqlFields, connection);
            cmd.Parameters.AddWithValue("T", table);

            var sb = new StringBuilder();
            sb.Append($"CREATE TABLE {table} (");

            using var r = cmd.ExecuteReader();

            bool first = true;
            while (r.Read())
            {
                if (!first)
                    sb.Append(", ");
                first = false;

                string? field = r["FIELD_NAME"].ToString();
                string? domainName = r["DOMAIN_NAME"].ToString();

                if (field == null && domainName == null)
                    throw new Exception("Nieobsługiwany przypadek: nazwa pola i domeny są null.");

                int fieldType = Convert.ToInt32(r["RDB$FIELD_TYPE"]);
                int length = Convert.ToInt32(r["RDB$FIELD_LENGTH"]);
                int charLength = r["RDB$CHARACTER_LENGTH"] is DBNull ? -1 : Convert.ToInt32(r["RDB$CHARACTER_LENGTH"]);
                int scale = Convert.ToInt32(r["RDB$FIELD_SCALE"]);
                int charsetId = r["RDB$CHARACTER_SET_ID"] is DBNull ? -1 : Convert.ToInt32(r["RDB$CHARACTER_SET_ID"]);

                string? defaultSrc = r["RDB$DEFAULT_SOURCE"] as string;
                string? checkSrc = r["RDB$VALIDATION_SOURCE"] as string;
                bool notNull = r["RDB$NULL_FLAG"] != DBNull.Value;

                string? dataType = !string.IsNullOrWhiteSpace(domainName)
                    ? domainName
                    : GetDataType(fieldType, length, charLength, scale);

                sb.Append($"{field} {dataType}");

                if (charsetId > 0)
                    sb.Append($" CHARACTER SET {GetCharsetName(charsetId)}");

                if (!string.IsNullOrWhiteSpace(defaultSrc))
                    sb.Append($" {defaultSrc.Trim()}");

                if (!string.IsNullOrWhiteSpace(checkSrc))
                    sb.Append($" {checkSrc.Trim()}");

                if (notNull)
                    sb.Append(" NOT NULL");
            }

            sb.AppendLine();
            sb.AppendLine(");");

            return sb.ToString();
        }

        private static void ExportProcedures(FbConnection connection, string outputDirectory)
        {
            string sql = @"
            SELECT TRIM(RDB$PROCEDURE_NAME) AS NAME
            FROM RDB$PROCEDURES
            WHERE RDB$SYSTEM_FLAG = 0
            ORDER BY NAME";

            Directory.CreateDirectory(outputDirectory);
            var proceduresDir = Path.Combine(outputDirectory, ProcedureScriptsSubdir);
            Directory.CreateDirectory(proceduresDir);

            using var cmd = new FbCommand(sql, connection);
            using var r = cmd.ExecuteReader();

            while (r.Read())
            {
                string name = (r["NAME"] as string)?.Trim() ?? throw new Exception("Nazwa procedury jest null.");

                var procText = BuildProcedureDefinition(connection, name);

                var fileName = $"procedure_{SanitizeFileName(name)}.sql";
                var filePath = Path.Combine(proceduresDir, fileName);
                File.WriteAllText(filePath, procText, Encoding.UTF8);
            }
        }

        private static string BuildProcedureDefinition(FbConnection connection, string procedureName)
        {
            string sqlProc = @"
            SELECT RDB$PROCEDURE_SOURCE
            FROM RDB$PROCEDURES
            WHERE RDB$PROCEDURE_NAME = @NAME";


            string sqlInputParams = @"
            SELECT 
                TRIM(PP.RDB$PARAMETER_NAME) AS PARAM_NAME,
                TRIM(PP.RDB$FIELD_SOURCE) AS DOMAIN_NAME,
                F.RDB$FIELD_TYPE,
                F.RDB$FIELD_LENGTH,
                F.RDB$CHARACTER_LENGTH,
                F.RDB$FIELD_SCALE,
                F.RDB$CHARACTER_SET_ID,
                PP.RDB$PARAMETER_NUMBER
            FROM RDB$PROCEDURE_PARAMETERS PP
            JOIN RDB$FIELDS F ON F.RDB$FIELD_NAME = PP.RDB$FIELD_SOURCE
            WHERE PP.RDB$PROCEDURE_NAME = @NAME AND PP.RDB$PARAMETER_TYPE = 0
            ORDER BY PP.RDB$PARAMETER_NUMBER";


            string sqlOutputParams = @"
            SELECT 
                TRIM(PP.RDB$PARAMETER_NAME) AS PARAM_NAME,
                TRIM(PP.RDB$FIELD_SOURCE) AS DOMAIN_NAME,
                F.RDB$FIELD_TYPE,
                F.RDB$FIELD_LENGTH,
                F.RDB$CHARACTER_LENGTH,
                F.RDB$FIELD_SCALE,
                F.RDB$CHARACTER_SET_ID,
                PP.RDB$PARAMETER_NUMBER
            FROM RDB$PROCEDURE_PARAMETERS PP
            JOIN RDB$FIELDS F ON F.RDB$FIELD_NAME = PP.RDB$FIELD_SOURCE
            WHERE PP.RDB$PROCEDURE_NAME = @NAME AND PP.RDB$PARAMETER_TYPE = 1
            ORDER BY PP.RDB$PARAMETER_NUMBER";

            var sb = new StringBuilder();
            sb.AppendLine($"CREATE PROCEDURE {procedureName}");


            using (var cmd = new FbCommand(sqlInputParams, connection))
            {
                cmd.Parameters.AddWithValue("NAME", procedureName);
                using var r = cmd.ExecuteReader();

                bool first = true;
                while (r.Read())
                {
                    if (first)
                    {
                        sb.Append("(");
                        first = false;
                    }
                    else
                    {
                        sb.Append(", ");
                    }

                    string paramName = r["PARAM_NAME"].ToString()!;
                    string domainName = r["DOMAIN_NAME"].ToString()!;

                    string dataType;
                    if (domainName.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase))
                    {
                        int fieldType = Convert.ToInt32(r["RDB$FIELD_TYPE"]);
                        int length = Convert.ToInt32(r["RDB$FIELD_LENGTH"]);
                        int charLength = r["RDB$CHARACTER_LENGTH"] is DBNull
                            ? -1
                            : Convert.ToInt32(r["RDB$CHARACTER_LENGTH"]);
                        int scale = Convert.ToInt32(r["RDB$FIELD_SCALE"]);
                        int charsetId = r["RDB$CHARACTER_SET_ID"] is DBNull
                            ? -1
                            : Convert.ToInt32(r["RDB$CHARACTER_SET_ID"]);

                        dataType = GetDataType(fieldType, length, charLength, scale)!;

                        sb.AppendLine();
                        sb.Append($"    {paramName} {dataType}");

                        if (charsetId > 0)
                            sb.Append($" CHARACTER SET {GetCharsetName(charsetId)}");
                    }
                    else
                    {
                        dataType = domainName;
                        sb.AppendLine();
                        sb.Append($"    {paramName} {dataType}");
                    }
                }

                if (!first)
                    sb.AppendLine(")");
            }

            using (var cmd = new FbCommand(sqlOutputParams, connection))
            {
                cmd.Parameters.AddWithValue("NAME", procedureName);
                using var r = cmd.ExecuteReader();

                bool first = true;
                while (r.Read())
                {
                    if (first)
                    {
                        sb.AppendLine("RETURNS (");
                        first = false;
                    }
                    else
                    {
                        sb.Append(", ");
                    }

                    string paramName = r["PARAM_NAME"].ToString()!;
                    string domainName = r["DOMAIN_NAME"].ToString()!;

                    string dataType;
                    if (domainName.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase))
                    {
                        int fieldType = Convert.ToInt32(r["RDB$FIELD_TYPE"]);
                        int length = Convert.ToInt32(r["RDB$FIELD_LENGTH"]);
                        int charLength = r["RDB$CHARACTER_LENGTH"] is DBNull
                            ? -1
                            : Convert.ToInt32(r["RDB$CHARACTER_LENGTH"]);
                        int scale = Convert.ToInt32(r["RDB$FIELD_SCALE"]);
                        int charsetId = r["RDB$CHARACTER_SET_ID"] is DBNull
                            ? -1
                            : Convert.ToInt32(r["RDB$CHARACTER_SET_ID"]);

                        dataType = GetDataType(fieldType, length, charLength, scale)!;

                        sb.AppendLine();
                        sb.Append($"    {paramName} {dataType}");

                        if (charsetId > 0)
                            sb.Append($" CHARACTER SET {GetCharsetName(charsetId)}");
                    }
                    else
                    {
                        dataType = domainName;
                        sb.AppendLine();
                        sb.Append($"    {paramName} {dataType}");
                    }
                }

                if (!first)
                {
                    sb.AppendLine();
                    sb.AppendLine(")");
                }
            }

            using (var cmd = new FbCommand(sqlProc, connection))
            {
                cmd.Parameters.AddWithValue("NAME", procedureName);
                using var r = cmd.ExecuteReader();

                if (r.Read())
                {
                    string source = r["RDB$PROCEDURE_SOURCE"] as string ?? string.Empty;
                    sb.AppendLine("AS");
                    sb.AppendLine(source.Trim());
                }
            }

            if (!sb.ToString().TrimEnd().EndsWith(";"))
                sb.AppendLine(";");

            return sb.ToString();
        }


        private static string SanitizeFileName(string name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var sb = new StringBuilder(name.Length);
            foreach (var ch in name)
            {
                sb.Append(invalid.Contains(ch) ? '_' : ch);
            }

            return sb.ToString();
        }

        #endregion

        #region Aktualizowanie bazy danych

        /// <summary>
        /// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void UpdateDatabase(string connectionString, string scriptsDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Wykonaj skrypty z katalogu scriptsDirectory (tylko obsługiwane elementy).
            // 3) Zadbaj o poprawną kolejność i bezpieczeństwo zmian.
            int executed = 0;
            
            using var connection = new FbConnection(connectionString);
            connection.Open();

            FbTransaction? transaction = null;
            try
            {
                transaction = connection.BeginTransaction();

                // 1) Domains
                var domainsFolder = Path.Combine(scriptsDirectory, DomainScriptsSubdir);
                var r1 = RunFolderSqlFiles(connection, domainsFolder, BuildDomainsFromFile, transaction);
                executed += r1.Executed;

                // 2) Tables
                var tablesFolder = Path.Combine(scriptsDirectory, TableScriptsSubdir);
                var r2 = RunFolderSqlFiles(connection, tablesFolder, ExecuteSqlFile, transaction);
                executed += r2.Executed;

                // 3) Procedures
                var procsFolder = Path.Combine(scriptsDirectory, ProcedureScriptsSubdir);
                var r3 = RunFolderSqlFiles(connection, procsFolder, ExecuteSqlFile, transaction);
                executed += r3.Executed;

                transaction.Commit();
                
                Console.WriteLine($"Skrypty załadowane poprawnie: {executed}");
            }
            catch (Exception e)
            {
                try
                {
                    transaction.Rollback();
                }
                catch
                {
                    Console.WriteLine("Błąd podczas wycofywania transakcji:");
                    throw;
                }
                throw;
            }
            finally
            {
                connection.Close();
            }
        }

        #endregion
        
        private static (int Executed, List<string> Failed) RunFolderSqlFiles(FbConnection connection, string folderPath, Action<FbConnection, string, FbTransaction?> executor, FbTransaction? transaction = null)
        {
            var failed = new List<string>();
            int executed = 0;

            if (Directory.Exists(folderPath))
            {
                foreach (var file in Directory.GetFiles(folderPath, "*.sql")
                             .OrderBy(f => f, StringComparer.OrdinalIgnoreCase))
                {
                    try
                    {
                        executor(connection, file, transaction);
                        executed++;
                    }
                    catch (Exception ex)
                    {
                        failed.Add($"{file} -> {ex.Message}");

                        if (transaction != null)
                        {
                            throw; 
                        }
                        
                        Console.WriteLine($"Błąd przy wczytywaniu pliku: {file}: {ex.Message}");
                    }
                }
            }

            return (executed, failed);
        }
        
        private static void BuildDomainsFromFile(FbConnection connection, string sqlFile, FbTransaction? transaction = null)
        {
            using var reader = new StreamReader(sqlFile);

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (line == null)
                    continue;

                using var cmd = new FbCommand(line, connection);
                if (transaction != null)
                    cmd.Transaction = transaction;
                cmd.ExecuteNonQuery();
            }
        }

        private static void ExecuteSqlFile(FbConnection connection, string sqlFile, FbTransaction? transaction = null)
        {
            var sql = File.ReadAllText(sqlFile);

            using var cmd = new FbCommand(sql, connection);
            if (transaction != null)
                cmd.Transaction = transaction;
            cmd.ExecuteNonQuery();
        }
    }
}