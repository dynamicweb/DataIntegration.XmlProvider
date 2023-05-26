using Dynamicweb.Core;
using Dynamicweb.Core.Helpers;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.Ecommerce.Common;
using Dynamicweb.Ecommerce.International;
using Dynamicweb.Ecommerce.Orders;
using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using Dynamicweb.Security.UserManagement.Common.CustomFields;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Xsl;

namespace Dynamicweb.DataIntegration.Providers.XmlProvider
{
    [AddInName("Dynamicweb.DataIntegration.Providers.Provider"), AddInLabel("XML Provider"), AddInDescription("XML provider"), AddInIgnore(false)]
    public class XmlProvider : BaseProvider, IDropDownOptions
    {
        private Schema _schema;
        private XmlWriter _xmlWriter;
        private XmlReader _xmlReader;
        private string _destinationFolder = "/Files";
        private string _sourceFolder = "/Files";
        private const string XmlExtension = ".xml";
        private string _sourceFileName;
        private string _destinationFileName;
        private string _xslTransformationTimestamp;
        private DateTime _timeStamp;
        private List<string> _sourceFiles = new List<string>();
        private Dictionary<string, string> _sourceFileArchievedFileDictionary = new Dictionary<string, string>();

        private string SourceFileFromUrl;

        public XmlWriter XmlWriter
        {
            get
            {
                if (_xmlWriter == null)
                {
                    if (!string.IsNullOrEmpty(WorkingDirectory))
                    {
                        _xmlWriter = XmlWriter.Create(_workingDirectory.CombinePaths(DestinationFolder, GetDestinationFile()), new XmlWriterSettings { Encoding = this.Encoding, NewLineHandling = NewLineHandling.Replace, Indent = true });
                    }
                    else
                    {
                        _xmlWriter = XmlWriter.Create(GetDestinationFile(), new XmlWriterSettings { Encoding = this.Encoding, NewLineHandling = NewLineHandling.Replace, Indent = true });
                    }
                }
                return _xmlWriter;
            }
        }

        [AddInParameter("Source folder"), AddInParameterEditor(typeof(FolderSelectEditor), "folder=/Files/"), AddInParameterGroup("Source")]
        public string SourceFolder
        {
            get
            { return _sourceFolder; }
            set
            { _sourceFolder = value; }
        }

        [AddInParameter("Source file"), AddInParameterEditor(typeof(FileManagerEditor), "folder=/Files/;Tooltip=Selecting a source file will override source folder selection"), AddInParameterGroup("Source")]
        public string SourceFile
        {
            get
            {
                return _sourceFileName;
            }
            set
            {
                _sourceFileName = value;
            }
        }

        [AddInParameter("Delete source file"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public bool DeleteSourceFile { get; set; }

        [AddInParameter("XSL file"), AddInParameterEditor(typeof(FileManagerEditor), "folder=/Files/;extensions=xsl,xslt"), AddInParameterGroup("Source")]
        public string XslFile { get; set; }

        [AddInParameter("Destination XSL file"), AddInParameterEditor(typeof(FileManagerEditor), "folder=/Files/;extensions=xsl,xslt"), AddInParameterGroup("Destination")]
        public string DestinationXslFile { get; set; }

        public override bool SchemaIsEditable
        {
            get { return true; }
        }

        private bool XslTransform
        {
            get
            { return !string.IsNullOrEmpty(XslFile); }
        }

        [AddInParameter("Destination file"), AddInParameterEditor(typeof(TextParameterEditor), $"append={XmlExtension};required"), AddInParameterGroup("Destination")]
        public string DestinationFile
        {
            get
            {
                return Path.GetFileNameWithoutExtension(_destinationFileName);
            }
            set
            {
                _destinationFileName = Path.GetFileNameWithoutExtension(value);
            }
        }

        [AddInParameter("Destination folder"), AddInParameterEditor(typeof(FolderSelectEditor), "folder=/Files/"), AddInParameterGroup("Destination")]
        public string DestinationFolder
        {
            get
            { return _destinationFolder; }
            set
            { _destinationFolder = value; }
        }

        public Encoding Encoding { get; set; }

        [AddInParameter("Destination encoding"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=false"), AddInParameterGroup("Destination")]
        public string DestionationEncoding
        {
            get { return Encoding.EncodingName; }
            set
            {
                SetEncoding(value);
            }
        }

        [AddInParameter("Include timestamp in filename"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual bool IncludeTimestampInFileName { get; set; }

        [AddInParameter("Skip Troublesome rows"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
        public bool SkipTroublesomeRows { get; set; }

        [AddInParameter("Export Product Field Definitions"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
        public bool ExportProductFieldDefinitions { get; set; }

        [AddInParameter("Number format culture"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=true"), AddInParameterGroup("Destination")]
        public string ExportCultureInfo { get; set; }

        private readonly string DetectAutomaticallySeparator = "Detect automatically";
        private readonly string NoneDecimalSeparator = "Use system culture";
        private string _sourceDecimalSeparator;
        [AddInParameter("Source decimal separator"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=false"), AddInParameterGroup("Source")]
        public string SourceDecimalSeparator
        {
            get
            {
                if (string.IsNullOrEmpty(_sourceDecimalSeparator))
                {
                    return DetectAutomaticallySeparator;
                }
                else
                {
                    return _sourceDecimalSeparator;
                }
            }
            set
            {
                _sourceDecimalSeparator = value;
            }
        }

        [AddInParameter("Archive source files"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual bool ArchiveSourceFiles { get; set; }

        private string _workingDirectory = SystemInformation.MapPath("/Files/");

        public override string WorkingDirectory
        {
            get
            {
                return _workingDirectory;
            }
            set { _workingDirectory = value.Replace("\\", "/"); }
        }

        /// <summary>
        /// Flag for finishing the import in ImportDataAddIn. When True update index can be started.
        /// </summary>
        public bool ExportIsDone = true;

        /// <summary>
        /// Source xml table name options collection
        /// </summary>
        public Dictionary<string, Dictionary<string, string>> TableOptions = new Dictionary<string, Dictionary<string, string>>();

        private int XmlDeclarationLength
        {
            get
            {
                XmlDeclaration decl = new XmlDocument().CreateXmlDeclaration("1.0", null, null);
                return decl.ToString().Length;
            }
        }

        private void SetSourceFileFromUrl()
        {
            if (Parameters != null && Parameters.TryGetValue("SourceFile", out SourceFileFromUrl) && !string.IsNullOrEmpty(SourceFileFromUrl))
            {
                if (!string.IsNullOrEmpty(SourceFile))
                {
                    if (SourceFile.Contains("/"))
                    {
                        SourceFile = SourceFile.Substring(0, SourceFile.LastIndexOf("/")).CombinePaths(SourceFileFromUrl);
                    }
                    else
                    {
                        SourceFile = SourceFileFromUrl;
                    }
                }
            }
        }

        public override void Initialize()
        {
            SetSourceFileFromUrl();

            if (string.IsNullOrEmpty(SourceFile))
            {
                string srcFolderPath = WorkingDirectory.CombinePaths(SourceFolder).Replace("\\", "/");

                if (!Directory.Exists(srcFolderPath))
                {
                    throw new Exception("Source folder \"" + SourceFolder + "\" does not exist");
                }

                if (!string.IsNullOrEmpty(SourceFileFromUrl))
                {
                    string filePath = srcFolderPath.CombinePaths(SourceFileFromUrl);
                    if (!File.Exists(filePath))
                    {
                        throw new Exception("Source file from request \"" + filePath + "\" does not exist");
                    }
                }
            }
            else
            {
                string srcFilePath = GetSourceFilePath();

                if (!File.Exists(srcFilePath))
                {
                    Logger?.Error("Source file \"" + SourceFile + "\" does not exist");
                }
            }

            if (XslTransform && File.Exists(Path.Combine(WorkingDirectory, XslFile)))
            {
                XslCompiledTransform oTransform = new XslCompiledTransform();
                XsltSettings xsltSettings = new XsltSettings();
                xsltSettings.EnableScript = true;
                oTransform.Load(WorkingDirectory.CombinePaths(XslFile), xsltSettings, new XmlUrlResolver());

                if (string.IsNullOrEmpty(_xslTransformationTimestamp))
                {
                    _xslTransformationTimestamp = DateTime.Now.ToString("yyyyMMddHHmmssffff");
                }
                foreach (string f in GetSourceFiles())
                {
                    FileInfo fi = new FileInfo(f);
                    string path = string.Format("{0}\\{1}-{2}.xt", fi.DirectoryName, fi.Name.Substring(0, fi.Name.Length - fi.Extension.Length), _xslTransformationTimestamp);
                    oTransform.Transform(f, path);
                }
            }
        }
        public void WriteToSourceFile(string InputXML)
        {
            WorkingDirectory = SystemInformation.MapPath("/Files/");
            FilesFolderName = "Files";
            if (!string.IsNullOrEmpty(SourceFile))
            {
                //try to save the xml with its encoding
                XmlDocument doc = new XmlDocument();
                try
                {
                    doc.LoadXml(InputXML);
                    doc.Save(WorkingDirectory.CombinePaths(FilesFolderName, SourceFile));
                }
                catch (Exception)
                {
                    //if it is not correct xml - save without encoding
                    TextFileHelper.WriteTextFile(InputXML, WorkingDirectory.CombinePaths(FilesFolderName, SourceFile));
                }
            }
        }

        /// <summary>
        /// Returns the Destination File with timestamp if "IncludeTimestampInFileName" is enabled
        /// </summary>
        /// <returns></returns>
        private string GetDestinationFile()
        {
            string ret = $"{DestinationFile}{XmlExtension}";
            if (IncludeTimestampInFileName)
            {
                ret = Path.GetFileNameWithoutExtension(DestinationFile) + _timeStamp.ToString("yyyyMMdd-HHmmssFFFFFFF") + XmlExtension;
            }
            return ret;
        }

        public string ReadOutputFile()
        {

            return TextFileHelper.ReadTextFile(_workingDirectory.CombinePaths(DestinationFolder, GetDestinationFile()));
        }

        public override string ValidateSourceSettings()
        {
            if (string.IsNullOrEmpty(SourceFile) && string.IsNullOrEmpty(SourceFolder))
                return "No Source file neither folder are selected";

            if (string.IsNullOrEmpty(SourceFile))
            {
                string srcFolderPath = (WorkingDirectory.CombinePaths(SourceFolder)).Replace("\\", "/");

                if (!Directory.Exists(srcFolderPath))
                    return "Source folder \"" + SourceFolder + "\" does not exist";
            }
            else
            {
                string srcFilePath = GetSourceFilePath();

                if (!File.Exists(srcFilePath))
                {
                    return "Source file \"" + SourceFile + "\" does not exist";
                }
                else
                {
                    if (!srcFilePath.EndsWith(XmlExtension))
                    {
                        return "Source file \"" + SourceFile + "\" is not xml file";
                    }
                }
            }

            // check XSL file exists if set)
            if (!string.IsNullOrEmpty(XslFile) && !File.Exists(WorkingDirectory.CombinePaths(XslFile)))
                return "XSL file \"" + XslFile + "\" does not exist. WorkingDirectory - " + WorkingDirectory;

            if (!string.IsNullOrEmpty(SourceFile) && !string.IsNullOrEmpty(SourceFolder))
                return "Warning: In your XML Provider source, you selected both a source file and a source folder. The source folder selection will be ignored, and only the source file will be used.";

            return null;
        }

        private string GetSourceFilePath()
        {
            string srcFilePath = string.Empty;

            if (_sourceFileName.StartsWith(".."))
            {
                srcFilePath = WorkingDirectory.CombinePaths(_sourceFileName.TrimStart(new char[] { '.' })).Replace("\\", "/");
            }
            else
            {
                srcFilePath = WorkingDirectory.CombinePaths(FilesFolderName, _sourceFileName).Replace("\\", "/");
            }

            return srcFilePath;
        }

        public override string ValidateDestinationSettings()
        {
            string dstPath = (WorkingDirectory.CombinePaths(DestinationFolder)).Replace("\\", "/");

            if (!Directory.Exists(dstPath))
                return "Destination folder \"" + DestinationFolder + "\" does not exist";

            if (DestinationFile == "" || !ValidateFilename(DestinationFile))
                return "Destination file name is not valid.";
            // check output XSL file exists if set
            if (!string.IsNullOrEmpty(DestinationXslFile) && !File.Exists(WorkingDirectory.CombinePaths(DestinationXslFile)))
                return "XSL file \"" + DestinationXslFile + "\" does not exist";

            return "";
        }

        private bool ValidateFilename(string fileNameWithPath)
        {
            try
            {
                string fileName = Path.GetFileName(fileNameWithPath);
                string directory = Path.GetDirectoryName(fileNameWithPath);
                foreach (char c in Path.GetInvalidFileNameChars())
                {
                    if (fileName.Contains(Converter.ToString(c)))
                    {
                        return false;
                    }
                }

                foreach (char c in Path.GetInvalidPathChars())
                {
                    if (directory.Contains(Converter.ToString(c)))
                    {
                        return false;
                    }
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public override void LoadSettings(Job job)
        {
            if (!string.IsNullOrEmpty(SourceFile) && !File.Exists(GetSourceFilePath()))
            {
                throw new Exception("Source file \"" + SourceFile + "\" does not exist");
            }
            CheckSourceFilesChanging();
            if (ArchiveSourceFiles)
            {
                ArchiveInputFiles();
            }
            foreach (string f in GetSourceFiles(this.XslTransform))
            {
                if (File.Exists(f) && (new FileInfo(f).Length > XmlDeclarationLength))
                {
                    Logger.Log("reading configuration");
                    using (XmlReader configReader = XmlReader.Create(new StreamReader(f, true), new XmlReaderSettings() { CloseInput = true }))
                    {
                        while (configReader.Read())
                        {
                            if (configReader.NodeType == XmlNodeType.Element && configReader.Name == "tables")
                            {
                                string value = configReader.GetAttribute("ExportIsDone");
                                ExportIsDone = !string.IsNullOrEmpty(value) ? bool.Parse(value.ToLower()) : true;
                            }
                            if (configReader.NodeType == XmlNodeType.Element && configReader.Name == "table")
                            {
                                return;
                            }
                            if (configReader.NodeType == XmlNodeType.Element && configReader.Name == "config")
                            {
                                while (configReader.Read() && !(configReader.NodeType == XmlNodeType.EndElement && configReader.Name == "tables") && !(configReader.NodeType == XmlNodeType.Element && configReader.Name == "tables"))
                                {
                                    if (configReader.NodeType == XmlNodeType.Element && configReader.Name == "source")
                                    {
                                        while (configReader.Read() && !(configReader.NodeType == XmlNodeType.EndElement && configReader.Name == "source"))
                                        {
                                            if (configReader.Name == "setting")
                                            {
                                                configReader.MoveToAttribute("field");
                                                string fieldName = configReader.Value;
                                                configReader.MoveToElement();
                                                configReader.Read();
                                                string value = configReader.Value.Trim();
                                                job.UpdateSourceSetting(fieldName, value);
                                            }

                                        }
                                    }
                                    if (configReader.NodeType == XmlNodeType.Element && configReader.Name == "destination")
                                    {
                                        while (configReader.Read() && !(configReader.NodeType == XmlNodeType.EndElement && configReader.Name == "destination"))
                                        {
                                            if (configReader.Name == "setting")
                                            {
                                                configReader.MoveToAttribute("field");
                                                string fieldName = configReader.Value;
                                                configReader.MoveToElement();
                                                configReader.Read();
                                                string value = configReader.Value.Trim();
                                                job.UpdateDestinationSetting(fieldName, value);
                                            }
                                        }
                                    }
                                    if (configReader.NodeType == XmlNodeType.Element &&
                                        (configReader.Name == "customField" || configReader.Name == "productCustomField" ||
                                        configReader.Name == "orderCustomField" || configReader.Name == "orderLineCustomField" ||
                                        configReader.Name == "userCustomField" || configReader.Name == "categoryField"))
                                    {
                                        ImportCustomField(configReader);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        private IEnumerable<string> GetSourceFiles(bool transform = false)
        {
            string srcPath = string.Empty;
            IEnumerable<string> files = null;

            if (string.IsNullOrEmpty(_sourceFileName))
            {
                srcPath = (WorkingDirectory.CombinePaths(SourceFolder)).Replace("\\", "/");

                if (transform)
                {
                    files = Directory.EnumerateFiles(srcPath, "*" + _xslTransformationTimestamp + ".xt", SearchOption.TopDirectoryOnly).OrderBy(fileName => fileName);
                }
                else
                {
                    if (!string.IsNullOrEmpty(SourceFileFromUrl))
                    {
                        files = new List<string>() { srcPath.CombinePaths(SourceFileFromUrl) };
                    }
                    else
                    {
                        files = Directory.EnumerateFiles(srcPath, XmlExtension, SearchOption.TopDirectoryOnly).OrderBy(fileName => fileName);
                    }
                }
            }
            else
            {
                srcPath = GetSourceFilePath();

                if (transform)
                {
                    FileInfo fi = new FileInfo(srcPath);
                    string fname = string.Format("{0}\\{1}-{2}.xt", fi.DirectoryName, fi.Name.Substring(0, fi.Name.Length - fi.Extension.Length), _xslTransformationTimestamp);
                    files = new List<string>() { fname };
                }
                else
                    files = new List<string>() { (new FileInfo(srcPath)).FullName };
            }

            return files;
        }

        private void SetEncoding(string encoding)
        {
            switch (encoding)
            {
                case "Unicode":
                    Encoding = Encoding.Unicode;
                    break;
                case "Unicode (UTF-8)":
                    Encoding = Encoding.UTF8;
                    break;
                case "Unicode (UTF-32)":
                    Encoding = Encoding.UTF32;
                    break;
                case "US-ASCII":
                case "ASCII":
                    Encoding = Encoding.ASCII;
                    break;
                default:
                    break;
            }
        }

        public override void UpdateSourceSettings(ISource source)
        {
            XmlProvider newProvider = (XmlProvider)source;
            SkipTroublesomeRows = newProvider.SkipTroublesomeRows;
            SourceFolder = newProvider.SourceFolder;
            SourceFile = newProvider.SourceFile;
            DeleteSourceFile = newProvider.DeleteSourceFile;
            DestinationFile = newProvider.DestinationFile;
            ExportProductFieldDefinitions = newProvider.ExportProductFieldDefinitions;
            Encoding = newProvider.Encoding;
            XslFile = newProvider.XslFile;
            DestionationEncoding = newProvider.DestionationEncoding;
            DestinationFolder = newProvider.DestinationFolder;
            DestinationXslFile = newProvider.DestinationXslFile;
            SourceDecimalSeparator = newProvider.SourceDecimalSeparator;
            ExportCultureInfo = newProvider.ExportCultureInfo;
            IncludeTimestampInFileName = newProvider.IncludeTimestampInFileName;
            ArchiveSourceFiles = newProvider.ArchiveSourceFiles;
        }

        public override void UpdateDestinationSettings(IDestination destination)
        {
            ISource newProvider = (ISource)destination;
            UpdateSourceSettings(newProvider);
        }
        Schema schemaFromDisk;
        public override Schema GetOriginalSourceSchema()
        {
            if (schemaFromDisk != null)
                return schemaFromDisk;

            Schema result = new Schema();
            Initialize();

            foreach (string f in GetSourceFiles(this.XslTransform))
            {
                if (File.Exists(f) && (new FileInfo(f).Length > XmlDeclarationLength))
                {
                    XmlReader schemaReader = null;
                    try
                    {
                        schemaReader = XmlReader.Create(new StreamReader(f, true), new XmlReaderSettings() { CloseInput = true });
                    }
                    catch (Exception ex)
                    {
                        Logger?.Error(string.Format("GetOriginalSourceSchema error reading file: {0} message: {1} stack: {2}", f, ex.Message, ex.StackTrace));
                    }
                    if (schemaReader != null)
                    {
                        using (schemaReader)
                        {
                            while (schemaReader.Read())
                            {
                                if (schemaReader.NodeType == XmlNodeType.Element && schemaReader.Name.ToLower() == "tables")
                                {
                                    while (schemaReader.Read() && !(schemaReader.NodeType == XmlNodeType.EndElement && schemaReader.Name.ToLower() == "tables"))
                                        if (schemaReader.NodeType == XmlNodeType.Element && schemaReader.Name.ToLower() == "table")
                                            AddTableToSchema(result, schemaReader);
                                }
                            }
                        }
                    }
                }
            }
            if (!JobIsGoingToRun && XslTransform)
            {
                var sourcefiles = GetSourceFiles(this.XslTransform);
                foreach (var file in sourcefiles)
                {
                    File.Delete(file);
                }
            }

            //todo
            schemaFromDisk = result;
            return result;
        }

        public override Schema GetOriginalDestinationSchema()
        {
            return GetSchema();
        }

        public override void OverwriteSourceSchemaToOriginal()
        {
            _schema = GetOriginalSourceSchema();
        }

        public override void OverwriteDestinationSchemaToOriginal()
        {
        }

        public override Schema GetSchema()
        {
            if (_schema == null)
                _schema = GetOriginalSourceSchema();

            return _schema;
        }

        public override string Serialize()
        {
            XDocument document = new XDocument(new XDeclaration("1.0", "utf-8", string.Empty));
            XElement root = new XElement("Parameters");
            document.Add(root);
            root.Add(CreateParameterNode(GetType(), "Source folder", SourceFolder));
            root.Add(CreateParameterNode(GetType(), "Destination folder", DestinationFolder));
            root.Add(CreateParameterNode(GetType(), "Source file", SourceFile));
            root.Add(CreateParameterNode(GetType(), "Delete source file", DeleteSourceFile.ToString()));
            root.Add(CreateParameterNode(GetType(), "Destination file", DestinationFile));
            root.Add(CreateParameterNode(GetType(), "XSL file", XslFile));
            root.Add(CreateParameterNode(GetType(), "Destination encoding", DestionationEncoding));
            root.Add(CreateParameterNode(GetType(), "Skip Troublesome rows", SkipTroublesomeRows.ToString()));
            root.Add(CreateParameterNode(GetType(), "Export Product Field Definitions", ExportProductFieldDefinitions.ToString()));
            root.Add(CreateParameterNode(GetType(), "Destination folder", DestinationFolder));
            root.Add(CreateParameterNode(GetType(), "Destination XSL file", DestinationXslFile));
            root.Add(CreateParameterNode(GetType(), "Source decimal separator", _sourceDecimalSeparator));
            root.Add(CreateParameterNode(GetType(), "Number format culture", ExportCultureInfo));
            root.Add(CreateParameterNode(GetType(), "Include timestamp in filename", IncludeTimestampInFileName.ToString()));
            root.Add(CreateParameterNode(GetType(), "Archive source files", ArchiveSourceFiles.ToString()));
            return document.ToString();
        }

        private void AddTableToSchema(Schema schema, XmlReader schemaReader)
        {
            schemaReader.MoveToAttribute("tableName");
            string tablename = schemaReader.Value;

            FillMappingOptions(tablename, schemaReader, null);

            schemaReader.MoveToElement();
            Table table = schema.AddTable(tablename);
            if (schemaReader.IsEmptyElement)
                return;

            string itemName = "item";
            bool exist = true;

            while (exist && schemaReader.Name != itemName)
                exist = schemaReader.Read();

            while (exist && (exist = schemaReader.Read()) && !(schemaReader.NodeType == XmlNodeType.EndElement && schemaReader.Name == itemName))
            {
                if (schemaReader.NodeType == XmlNodeType.Element)
                {
                    schemaReader.MoveToAttribute("columnName");
                    table.AddColumn(new Column(schemaReader.Value, typeof(string), table));
                    schemaReader.MoveToElement();
                }
            }
            while (exist && (schemaReader.NodeType != XmlNodeType.EndElement || schemaReader.Name != "table"))
            {
                schemaReader.Skip();
                if (schemaReader.NodeType == XmlNodeType.None)
                    throw new XmlException("Unexpected EOF");
            }

        }

        private void FillMappingOptions(string tablename, XmlReader schemaReader, Mapping mapping)
        {
            if (schemaReader.AttributeCount > 1)
            {
                if (!TableOptions.ContainsKey(tablename))
                {
                    TableOptions.Add(tablename, new Dictionary<string, string>());
                }
                var options = TableOptions[tablename];
                while (schemaReader.MoveToNextAttribute())
                {
                    if (!options.ContainsKey(schemaReader.Name))
                    {
                        options.Add(schemaReader.Name, schemaReader.Value);
                    }
                }
                if (mapping != null && mapping.Options == null && mapping.SourceTable != null && TableOptions.ContainsKey(mapping.SourceTable.Name))
                {
                    mapping.Options = TableOptions[mapping.SourceTable.Name];
                }
            }
        }

        public XmlProvider()
        {
            Encoding = Encoding.UTF8;
            SkipTroublesomeRows = true;
            ExportProductFieldDefinitions = false;
            if (string.IsNullOrEmpty(FilesFolderName))
            {
                FilesFolderName = "Files";
            }
            ArchiveSourceFiles = false;
        }

        public XmlProvider(XmlNode xmlNode)
        {
            Encoding = Encoding.UTF8;
            SkipTroublesomeRows = true;
            ExportProductFieldDefinitions = false;
            ArchiveSourceFiles = false;
            SourceDecimalSeparator = NoneDecimalSeparator;
            FilesFolderName = "Files";

            foreach (XmlNode node in xmlNode.ChildNodes)
            {
                switch (node.Name)
                {
                    case "Encoding":
                        switch (node.FirstChild.Value)
                        {
                            case "Unicode":
                                Encoding = Encoding.Unicode;
                                break;
                            case "Unicode (UTF-8)":
                                Encoding = Encoding.UTF8;
                                break;
                            case "Unicode (UTF-32)":
                                Encoding = Encoding.UTF32;
                                break;
                            case "US-ASCII":
                            case "ASCII":
                                Encoding = Encoding.ASCII;
                                break;
                            default:
                                break;
                        }

                        break;
                    case "Schema":
                        _schema = new Schema(node);
                        break;
                    case "SkipTroublesomeRows":
                        SkipTroublesomeRows = node.FirstChild.Value == "True";
                        break;
                    case "ExportProductFieldDescriptions":
                        ExportProductFieldDefinitions = node.FirstChild.Value == "True";
                        break;
                    case "Filename":
                        if (node.HasChildNodes)
                            DestinationFile = node.FirstChild.Value;
                        SourceFile = node.FirstChild.Value;

                        break;
                    case "DestinationFile":
                        {
                            if (node.HasChildNodes)
                                DestinationFile = node.FirstChild.Value;

                            break;
                        }
                    case "SourceFile":
                        {
                            if (node.HasChildNodes)
                                SourceFile = node.FirstChild.Value;

                            break;
                        }
                    case "xslfile":
                        if (node.HasChildNodes)
                            XslFile = node.FirstChild.Value;
                        break;
                    case "DestinationXslfile":
                        if (node.HasChildNodes)
                            DestinationXslFile = node.FirstChild.Value;
                        break;
                    case "DestinationFolder":
                        {
                            if (node.HasChildNodes)
                                DestinationFolder = node.FirstChild.Value;

                            break;
                        }
                    case "SourceFolder":
                        {
                            if (node.HasChildNodes)
                                SourceFolder = node.FirstChild.Value;

                            break;
                        }
                    case "SourceDecimalSeparator":
                        if (node.HasChildNodes)
                            _sourceDecimalSeparator = node.FirstChild.Value;
                        break;
                    case "ExportCultureInfo":
                        if (node.HasChildNodes)
                            ExportCultureInfo = node.FirstChild.Value;
                        break;
                    case "DeleteSourceFile":
                        if (node.HasChildNodes)
                            DeleteSourceFile = node.FirstChild.Value == "True";
                        break;
                    case "IncludeTimestampInFileName":
                        if (node.HasChildNodes)
                            IncludeTimestampInFileName = node.FirstChild.Value == "True";
                        break;
                    case "ArchiveSourceFiles":
                        if (node.HasChildNodes)
                            ArchiveSourceFiles = node.FirstChild.Value == "True";
                        break;
                }
            }
        }

        public XmlProvider(string filename)
        {
            SourceFile = filename;
            Encoding = Encoding.UTF8;
            SkipTroublesomeRows = true;
            ExportProductFieldDefinitions = false;
            DestinationFile = filename;
            ArchiveSourceFiles = false;
        }

        public override void Close()
        {
            if (_xmlWriter != null)
            {
                XmlWriter.Flush();
                XmlWriter.Close();
                if (!string.IsNullOrEmpty(DestinationXslFile))
                {
                    MakeDestinationXslTransformation();
                }
            }

            File.Delete(SystemInformation.MapPath("/Files/xslTransformedInput.xml"));

            DeleteSourceFiles();
            DeleteArchievedFiles();
        }

        public override void SaveAsXml(XmlTextWriter textWriter)
        {
            textWriter.WriteElementString("DestinationFile", DestinationFile);
            textWriter.WriteElementString("SourceFile", SourceFile);
            textWriter.WriteElementString("xslfile", XslFile);
            textWriter.WriteElementString("Encoding", Encoding.EncodingName);
            textWriter.WriteElementString("SkipTroublesomeRows", SkipTroublesomeRows.ToString());
            textWriter.WriteElementString("ExportProductFieldDescriptions", ExportProductFieldDefinitions.ToString());
            textWriter.WriteElementString("SourceFolder", SourceFolder);
            textWriter.WriteElementString("DestinationFolder", DestinationFolder);
            textWriter.WriteElementString("DestinationXslfile", DestinationXslFile);
            textWriter.WriteElementString("SourceDecimalSeparator", _sourceDecimalSeparator);
            textWriter.WriteElementString("ExportCultureInfo", ExportCultureInfo);
            textWriter.WriteElementString("DeleteSourceFile", DeleteSourceFile.ToString());
            textWriter.WriteElementString("IncludeTimestampInFileName", IncludeTimestampInFileName.ToString());
            textWriter.WriteElementString("ArchiveSourceFiles", ArchiveSourceFiles.ToString());
            GetSchema().SaveAsXml(textWriter);
        }

        public override bool RunJob(Job job)
        {
            _timeStamp = DateTime.Now;
            ReplaceMappingConditionalsWithValuesFromRequest(job);

            Dictionary<string, object> sourceRow = null;
            try
            {
                XmlWriter.WriteStartElement("tables");

                if (ExportProductFieldDefinitions)
                    writeProductFieldDescriptions();

                CultureInfo ci = GetCultureInfo();

                foreach (Mapping map in job.Mappings)
                {
                    if (map.Active)
                    {
                        Logger.Log("Starting export of table: " + map.DestinationTable.Name);
                        XmlWriter.WriteStartElement("table");
                        XmlWriter.WriteAttributeString("tableName", map.DestinationTable.Name);

                        WriteMappingOptions(map);

                        XmlDestinationWriter writer = new XmlDestinationWriter(map, XmlWriter, SkipTroublesomeRows, ci, Logger);

                        using (ISourceReader reader = map.Source.GetReader(map))
                        {
                            int counter = 0;

                            while (!reader.IsDone())
                            {
                                sourceRow = reader.GetNext();
                                ProcessInputRow(map, sourceRow);
                                writer.Write(sourceRow);
                                counter += 1;

                                if (counter % 5000 == 0)
                                    Logger.Log(counter + " rows exported to table: " + map.DestinationTable.Name);
                            }
                        }

                        XmlWriter.WriteEndElement();
                        Logger.Log("Finished export of table: " + map.DestinationTable.Name);
                    }
                }
                XmlWriter.WriteEndElement();
                sourceRow = null;

                return true;
            }
            catch (EncoderFallbackException ex)
            {
                Logger.Log("job failed: Encoding error - " + ex.Message);
                return false;
            }
            catch (Exception ex)
            {
                string msg = ex.Message;
                LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {ex.Message} Stack: {ex.StackTrace}", ex);
                if (sourceRow != null)
                    msg += GetFailedSourceRowMessage(sourceRow);
                Logger.Log("job failed: " + msg);
                return false;
            }
            finally
            {
                Close();
                sourceRow = null;
            }
        }

        private void WriteMappingOptions(Mapping map)
        {
            if (map.Options != null)
            {
                foreach (KeyValuePair<string, string> kvp in map.Options)
                {
                    string value = Converter.ToString(kvp.Value);
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        XmlWriter.WriteAttributeString(kvp.Key, value);
                    }
                }
            }
        }

        private void writeProductFieldDescriptions()
        {
            XmlWriter.WriteStartElement("config");
            var productFields = Application.ProductFields;
            foreach (var productField in productFields)
            {
                XmlWriter.WriteStartElement("customField");
                XmlWriter.WriteAttributeString("name", productField.Name);
                XmlWriter.WriteAttributeString("systemName", productField.SystemName);
                XmlWriter.WriteAttributeString("templateName", productField.TemplateName);
                XmlWriter.WriteAttributeString("typeId", productField.TypeId.ToString());
                XmlWriter.WriteEndElement();
            }
            XmlWriter.WriteEndElement();
        }

        public override ISourceReader GetReader(Mapping mapping)
        {
            bool autoDetectDecimalSeparator = (_sourceDecimalSeparator == null) ? false : (_sourceDecimalSeparator == DetectAutomaticallySeparator);
            string decimalSeparator = null;
            //If source decimal separator is diffent from current culture separator - use source decimal separator
            if (!autoDetectDecimalSeparator && !string.IsNullOrEmpty(_sourceDecimalSeparator) && _sourceDecimalSeparator != NoneDecimalSeparator &&
                _sourceDecimalSeparator != CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator)
                decimalSeparator = _sourceDecimalSeparator;

            return new XmlSourceReader(this, mapping, decimalSeparator, autoDetectDecimalSeparator);
        }

        public XmlReader GetXmlReader(Mapping map)
        {
            if (_xmlReader != null)
            {
                if (SkipToBeginning(map))
                    return _xmlReader;
                else
                {
                    _xmlReader.Close();
                    _xmlReader = null;
                }
            }

            if (string.IsNullOrEmpty(SourceFile))
            {
                string srcFolderPath = (WorkingDirectory.CombinePaths(SourceFolder)).Replace("\\", "/");

                if (!Directory.Exists(srcFolderPath))
                {
                    Logger?.Error($"Source folder {SourceFolder} does not exist");
                    return null;
                }
            }
            else
            {
                string srcFilePath = GetSourceFilePath();

                if (!File.Exists(srcFilePath))
                {
                    Logger?.Error($"Source file {SourceFile} does not exist");
                    return null;
                }
            }

            FileInfo dstFi = null;

            if (map.Destination is XmlProvider)
            {
                XmlProvider prov = (XmlProvider)map.Destination;
                dstFi = new FileInfo(string.Format("{0}\\{1}\\{2}", prov.WorkingDirectory, prov.DestinationFolder, prov.GetDestinationFile()));
            }

            List<string> sourceFilesList = GetSourceFiles(XslTransform).ToList();
            foreach (string f in sourceFilesList)
            {
                if (dstFi != null)
                {
                    FileInfo srcFi = new FileInfo(f);

                    if (string.Compare(dstFi.FullName, srcFi.FullName, true) == 0)
                        continue;
                }
                if ((new FileInfo(f).Length > XmlDeclarationLength) && (sourceFilesList.Count == 1 || Context.Current == null || Context.Current.Items == null || Context.Current.Items[f] == null))
                {
                    _xmlReader = XmlReader.Create(new StreamReader(f, true), new XmlReaderSettings() { CloseInput = true });

                    if (SkipToBeginning(map))
                    {
                        if (Context.Current?.Items != null)
                        {
                            //save xml file as processed
                            Context.Current.Items[f] = true;
                        }
                        if (!_sourceFiles.Contains(f))
                        {
                            if (XslTransform)
                            {
                                _sourceFiles.Add(f.Replace($"-{_xslTransformationTimestamp}.xt", XmlExtension));
                            }
                            else
                            {
                                _sourceFiles.Add(f);
                            }
                        }
                        return _xmlReader;
                    }
                    else
                    {
                        _xmlReader.Close();
                        _xmlReader = null;
                    }
                }
                else
                {
                    _xmlReader = null;
                }
            }

            return XmlReader.Create(new StringReader("<tables><table tableName=\"" + map.SourceTable.Name + "\" /></tables>"));
        }

        private bool SkipToBeginning(Mapping map)
        {
            //initialize, if this is the first read from  this reader XmlReader
            if (_xmlReader.NodeType == XmlNodeType.None)
            {
                while (_xmlReader.Read())
                {
                    if (_xmlReader.NodeType == XmlNodeType.Element && _xmlReader.Name.ToLower() == "tables")
                    {
                        break;
                    }
                }
                //we are now on the Tables node, just before the first tableNode
            }
            string tableName = null;
            while (_xmlReader.Read())
            {
                if (_xmlReader.NodeType == XmlNodeType.Element && _xmlReader.Name == "table")
                {
                    _xmlReader.MoveToAttribute("tableName");
                    tableName = _xmlReader.Value;
                    FillMappingOptions(tableName, _xmlReader, map);
                    break;
                }
            }
            while (tableName != map.SourceTable.Name)
            {
                _xmlReader.MoveToElement();
                _xmlReader.Skip();
                _xmlReader.MoveToAttribute("tableName");
                tableName = _xmlReader.Value;
                if (_xmlReader.NodeType == XmlNodeType.Attribute)
                {
                    FillMappingOptions(tableName, _xmlReader, map);
                }
                if (_xmlReader.NodeType == XmlNodeType.None)
                {
                    return false;
                }
            }
            _xmlReader.MoveToElement();
            return true;
        }

        public Hashtable GetOptions(string name)
        {
            if (name == "Source decimal separator")
            {
                var options = new Hashtable
                              {
                                  {NoneDecimalSeparator, NoneDecimalSeparator},
                                  {DetectAutomaticallySeparator, DetectAutomaticallySeparator},
                                  {".", "."},
                                  {",", ","}
                              };
                return options;
            }
            else if (name == "Number format culture")
            {
                var options = new Hashtable();
                CountryCollection countries = Ecommerce.Services.Countries.GetCountries(); ;
                foreach (Country c in countries)
                {
                    if (!string.IsNullOrEmpty(c.CultureInfo) && !options.Contains(c.CultureInfo))
                        options.Add(c.CultureInfo, c.Code2);
                }
                return options;
            }
            else
            {
                var options = new Hashtable
                              {
                                  {"Unicode (UTF-8)", "UTF8"},
                                  {"Unicode", "Unicode"},
                                  {"US-ASCII", "ASCII"},
                                  {"Unicode (UTF-32)", "UTF32"}
                              };
                return options;
            }
        }

        private void MakeDestinationXslTransformation()
        {
            if (!File.Exists(WorkingDirectory + DestinationXslFile))
                return;

            string destinationXmlFilePath = WorkingDirectory;
            if (!string.IsNullOrEmpty(DestinationFolder))
            {
                destinationXmlFilePath = destinationXmlFilePath.CombinePaths(DestinationFolder, GetDestinationFile());
            }

            if (!File.Exists(destinationXmlFilePath))
                throw new Exception("File missing: " + DestinationFile);

            using (StringWriter sw = new StringWriter())
            {
                using (XmlReader reader = XmlReader.Create(destinationXmlFilePath))
                {
                    XslCompiledTransform oTransform = new XslCompiledTransform();
                    XsltSettings xsltSettings = new XsltSettings();
                    xsltSettings.EnableScript = true;
                    try
                    {
                        oTransform.Load(WorkingDirectory.CombinePaths(DestinationXslFile), xsltSettings, new XmlUrlResolver());
                    }
                    catch (Exception ex)
                    {
                        Logger.Log($"Error loading xslt: {ex.Message}");
                        throw;
                    }

                    XmlWriterSettings writerSettings = oTransform.OutputSettings != null ? oTransform.OutputSettings.Clone() : new XmlWriterSettings();
                    writerSettings.Encoding = Encoding;

                    using (XmlWriter writer = XmlWriter.Create(sw, writerSettings))
                    {
                        try
                        {
                            oTransform.Transform(reader, writer);
                        }
                        catch (Exception ex)
                        {
                            Logger.Log($"Error xml xslt transform: {ex.Message}");
                            throw;
                        }
                    }
                }

                XDocument xDoc = XDocument.Parse(sw.ToString());
                xDoc.Declaration.Encoding = Encoding.WebName;
                xDoc.Save(destinationXmlFilePath);
            }
        }

        #region Import Custom Fields

        private void ImportCustomField(XmlReader configReader)
        {
            try
            {
                if (configReader.Name == "customField" || configReader.Name == "productCustomField")
                    ImportProductField(configReader);
                if (configReader.Name == "orderCustomField")
                    ImportOrderCustomField(configReader);
                if (configReader.Name == "orderLineCustomField")
                    ImportOrderLineCustomField(configReader);
                if (configReader.Name == "userCustomField")
                    ImportUserCustomField(configReader);
                if (configReader.Name == "categoryField")
                    ImportCategoryField(configReader);
                configReader.MoveToElement();
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error importing custom field '{0}' : {1}.", configReader.Name, ex.Message));
            }
        }

        private void ImportProductField(XmlReader configReader)
        {
            var productField = new ProductField();
            configReader.MoveToAttribute("name");
            productField.Name = configReader.Value;
            configReader.MoveToAttribute("systemName");
            productField.SystemName = configReader.Value;
            configReader.MoveToAttribute("templateName");
            productField.TemplateName = configReader.Value;
            configReader.MoveToAttribute("typeId");
            var typeid = configReader.Value;
            productField.TypeId = int.Parse(typeid);

            productField.Save(string.Empty);

            //get newField from xml
            //get existingFields from db
            //if existing contains new, return, else create
            ProductField.GetProductFields();
        }

        private void ImportOrderCustomField(XmlReader configReader)
        {
            configReader.MoveToAttribute("systemName");
            string systemName = configReader.Value;
            if (!Ecommerce.Services.OrderFields.GetOrderFields().Any(of => of.SystemName == systemName))
            {
                OrderField orderField = new OrderField();
                configReader.MoveToAttribute("name");
                orderField.Name = configReader.Value;

                orderField.SystemName = systemName;

                configReader.MoveToAttribute("templateName");
                orderField.TemplateName = configReader.Value;
                configReader.MoveToAttribute("typeId");
                orderField.TypeId = int.Parse(configReader.Value);
                orderField.TypeName = Ecommerce.Services.FieldType.GetFieldTypes(true).First()?.Name;
                Ecommerce.Services.OrderFields.Save(orderField);
            }
        }

        private void ImportOrderLineCustomField(XmlReader configReader)
        {
            configReader.MoveToAttribute("name");
            string name = configReader.Value;
            configReader.MoveToAttribute("systemName");
            string systemName = configReader.Value;
            configReader.MoveToAttribute("length");
            int length = Converter.ToInt32(configReader.Value);
            OrderLineField orderLineField = new OrderLineField(systemName, name, length);
            Ecommerce.Services.OrderLineFields.Save(orderLineField);
        }

        private void ImportUserCustomField(XmlReader configReader)
        {
            configReader.MoveToAttribute("name");
            string name = configReader.Value;

            configReader.MoveToAttribute("systemName");
            string systemName = configReader.Value;

            Types type = Types.Text;
            if (configReader.MoveToAttribute("type"))
            {
                string strType = configReader.Value;
                type = (Types)Enum.Parse(typeof(Types), strType);
            }
            CustomField field = new CustomField(systemName, "AccessUser", type)
            {
                Name = name
            };
            field.Save();
        }

        private void ImportCategoryField(XmlReader configReader)
        {
            configReader.MoveToAttribute("id");
            string id = configReader.Value;

            configReader.MoveToAttribute("templateTag");
            string templateTag = configReader.Value;

            configReader.MoveToAttribute("typeId");
            string typeId = configReader.Value;

            configReader.MoveToAttribute("label");
            string label = configReader.Value;

            configReader.MoveToAttribute("categoryId");
            string categoryId = configReader.Value;
            var category = Ecommerce.Services.ProductCategories.GetCategoryById(categoryId);
            var categoryField = Ecommerce.Services.ProductCategoryFields.GetFieldById(categoryId, id);
            if (categoryField == null)
            {
                Ecommerce.Services.ProductCategories.AddFieldToCategory(category, id, label, templateTag, typeId,
                  string.Empty, FieldListPresentationType.RadioButtonList, 0, string.Empty);
            }
            Ecommerce.Services.ProductCategories.SaveCategory(category);
        }

        #endregion Import Custom Fields

        private CultureInfo GetCultureInfo()
        {
            CultureInfo result = null;

            if (!string.IsNullOrEmpty(ExportCultureInfo))
            {
                try
                {
                    result = CultureInfo.GetCultureInfo(ExportCultureInfo);
                }
                catch (Exception ex)
                {
                    if (Logger != null)
                        Logger.Log(string.Format("Error getting culture: {0}.", ex.Message));
                }
            }

            return result;
        }

        private void DeleteSourceFiles()
        {
            List<string> sourceFiles = new List<string>();
            if (this.DeleteSourceFile)
            {
                sourceFiles = GetSourceFiles().ToList();
            }
            if (this.XslTransform)
            {
                sourceFiles.AddRange(GetSourceFiles(this.XslTransform).ToList());
            }
            foreach (string file in sourceFiles)
            {
                if (SkipFile(file))
                {
                    continue;
                }

                if (File.Exists(file))
                {
                    try
                    {
                        File.Delete(file);
                    }
                    catch (Exception ex)
                    {
                        Logger.Log(string.Format("Can't delete source file: {0}. Error: {1}", file, ex.Message));
                    }
                }
            }
        }

        private bool SkipFile(string file)
        {
            if (string.IsNullOrEmpty(_sourceFileName))
            {
                //Skip not used xml files from the Source folder
                if (file.EndsWith(XmlExtension) && !_sourceFiles.Contains(file))
                {
                    return true;
                }
            }
            return false;
        }

        private void ArchiveInputFiles()
        {
            string timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmssFFFFFFF");
            foreach (string file in GetSourceFiles())
            {
                if (File.Exists(file))
                {
                    try
                    {
                        string directory = Path.GetDirectoryName(file);
                        if (!string.IsNullOrEmpty(directory))
                        {
                            string archiveDirectory = directory.CombinePaths("Archive");
                            Directory.CreateDirectory(archiveDirectory);
                            string newFile = archiveDirectory.CombinePaths(Path.GetFileNameWithoutExtension(file) + "_" + timestamp + Path.GetExtension(file));
                            if (File.Exists(newFile))
                            {
                                newFile = archiveDirectory.CombinePaths(Path.GetFileNameWithoutExtension(file) + "_" + DateTime.Now.ToString("yyyyMMdd-HHmmssFFFFFFF") + Path.GetExtension(file));
                            }
                            File.Copy(file, newFile);
                            _sourceFileArchievedFileDictionary.Add(file, newFile);
                        }
                        else
                        {
                            Logger.Log(string.Format("Archive error: can't find directory for the source file: {0}.", file));
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Log(string.Format("Can't archive source file: {0}. Error: {1}", file, ex.Message));
                    }
                }
            }
        }

        private void DeleteArchievedFiles()
        {
            foreach (string file in _sourceFileArchievedFileDictionary.Keys)
            {
                if (SkipFile(file) && File.Exists(_sourceFileArchievedFileDictionary[file]))
                {
                    File.Delete(_sourceFileArchievedFileDictionary[file]);
                }
            }
        }

        private void CheckSourceFilesChanging()
        {
            IEnumerable<string> files = GetSourceFiles().Distinct();
            if (files != null && files.Count() > 0)
            {
                Logger.Log("Start checking input files changing");

                Dictionary<string, long> fileSizeDictionary = new Dictionary<string, long>();
                foreach (string file in files)
                {
                    if (File.Exists(file))
                    {
                        FileInfo fileInfo = new FileInfo(file);
                        fileSizeDictionary.Add(file, fileInfo.Length);
                    }
                }
                System.Threading.Thread.Sleep(5 * 1000);
                foreach (string file in fileSizeDictionary.Keys)
                {
                    FileInfo changedFileInfo = new FileInfo(file);
                    if (changedFileInfo != null && changedFileInfo.Length != fileSizeDictionary[file])
                    {
                        throw new Exception(string.Format("Source file: '{0}' is still updating", file));
                    }
                }
                Logger.Log("Finish checking input files changing");
            }
        }
    }
}
