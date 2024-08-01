using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Xml;

namespace Dynamicweb.DataIntegration.Providers.XmlProvider;

class XmlDestinationWriter : IDestinationWriter
{
    private readonly System.Xml.XmlWriter _xmlWriter;
    private readonly bool _skipTroublesomeRows;
    private readonly CultureInfo _cultureInfo;
    private readonly ILogger _logger;
    private readonly IEnumerable<ColumnMapping> _columnMappingCollection;

    public Mapping Mapping { get; }

    public XmlDestinationWriter(Mapping mapping, System.Xml.XmlWriter xmlWriter, bool skipTroublesomeRows, CultureInfo cultureInfo, ILogger logger)
    {
        _logger = logger;
        Mapping = mapping;
        _xmlWriter = xmlWriter;
        _skipTroublesomeRows = skipTroublesomeRows;
        _cultureInfo = cultureInfo;
        _columnMappingCollection = Mapping.GetColumnMappings().Where(m => m.Active && m.SourceColumn is not null);
    }

    public void Write(Dictionary<string, object> p)
    {
        if (_skipTroublesomeRows)
        {
            foreach (KeyValuePair<string, object> obj in p)
            {

                if (!StringIsFreeOfTroublesomeCharacters(obj.ToString()))
                {
                    System.Diagnostics.Debug.WriteLine("string '" + obj + "' contains a character that is not allowed in XML");
                    StringBuilder builder = new StringBuilder();
                    foreach (KeyValuePair<string, object> pair in p)
                    {
                        builder.Append(pair.Key).Append(":").Append(pair.Value.ToString()).Append(',');
                    }
                    string result = builder.ToString();
                    // Remove the final delimiter
                    result = result.TrimEnd(',');
                    _logger.Log("Encountered invalid character in string '" + obj + "' in table " + Mapping.DestinationTable.Name + ". Skipping row: " + result + ".");
                    return; //invalid string, return without writing a node
                }
            }
        }
        try
        {

            _xmlWriter.WriteStartElement("item");
            _xmlWriter.WriteAttributeString("table", Mapping.DestinationTable.Name);
            foreach (var mapping in _columnMappingCollection)
            {
                if (p.ContainsKey(mapping.SourceColumn.Name))
                {
                    _xmlWriter.WriteStartElement("column");
                    _xmlWriter.WriteAttributeString("columnName", mapping.DestinationColumn.Name);

                    if (mapping.HasScriptWithValue)
                    {
                        if (mapping.SourceColumn.Type == typeof(DateTime))
                        {
                            DateTime theDate = DateTime.Parse(mapping.GetScriptValue());
                            _xmlWriter.WriteCData(theDate.ToString("dd-MM-yyyy HH:mm:ss:fff", _cultureInfo));
                        }
                        else if (mapping.SourceColumn.Type == typeof(decimal) ||
                            mapping.SourceColumn.Type == typeof(double) ||
                            mapping.SourceColumn.Type == typeof(float))
                        {
                            string value = ValueFormatter.GetFormattedValue(mapping.GetScriptValue(), _cultureInfo, mapping.ScriptType, mapping.ScriptValue);
                            _xmlWriter.WriteCData(value);
                        }
                        else
                        {
                            _xmlWriter.WriteCData(mapping.GetScriptValue());
                        }
                    }
                    else if (p[mapping.SourceColumn.Name] is DBNull || p[mapping.SourceColumn.Name] is null)
                    {
                        _xmlWriter.WriteAttributeString("isNull", "true");
                    }
                    else if (mapping.SourceColumn.Type == typeof(DateTime))
                    {
                        if (DateTime.TryParse(mapping.ConvertInputValueToOutputValue(p[mapping.SourceColumn.Name])?.ToString(), out var theDateTime))
                        {
                            _xmlWriter.WriteCData(theDateTime.ToString("dd-MM-yyyy HH:mm:ss:fff", _cultureInfo));
                        }
                        else
                        {
                            _xmlWriter.WriteCData(DateTime.MinValue.ToString("dd-MM-yyyy HH:mm:ss:fff", CultureInfo.InvariantCulture));
                        }
                    }
                    else
                    {
                        _xmlWriter.WriteCData(string.Format(_cultureInfo, "{0}", mapping.ConvertInputValueToOutputValue(p[mapping.SourceColumn.Name])));
                    }
                    _xmlWriter.WriteEndElement();
                }
                else
                {
                    throw new Exception(BaseDestinationWriter.GetRowValueNotFoundMessage(p, mapping.SourceColumn.Table.Name, mapping.SourceColumn.Name));
                }
            }
            _xmlWriter.WriteEndElement();
        }
        catch (ArgumentException)
        {
            string problemRow = p.Cast<object>().Aggregate("", (current, obj) => current + obj.ToString() + ", ");

            System.Diagnostics.Debug.WriteLine(Mapping.SourceTable.Name + " - " + problemRow.Substring(0, problemRow.Length - 2));
            throw;  //TODO: Fail if needed, log otherwise
        }
    }


    public void Close()
    {
        _xmlWriter.WriteEndElement();
    }

    /// <summary>
    /// Checks if the inString can be converted to Xml
    /// </summary>
    /// <param name="inString">The string to process</param>
    /// <returns>True if it can be converted else false</returns>
    private static bool StringIsFreeOfTroublesomeCharacters(string inString)
    {
        if (inString == null) return true;
        try
        {
            //Returns the passed-in string if all the characters and surrogate pair characters in the string argument are valid XML characters,
            //otherwise an XmlException is thrown with information on the first invalid character encountered.
            XmlConvert.VerifyXmlChars(inString);
            return true;
        }
        catch (XmlException)
        {
            return false;
        }
    }
}
