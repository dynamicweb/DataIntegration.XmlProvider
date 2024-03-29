﻿using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Dynamicweb.DataIntegration.Providers.XmlProvider;

class XmlDestinationWriter : IDestinationWriter
{
    private readonly System.Xml.XmlWriter _xmlWriter;
    private readonly bool _skipTroublesomeRows;
    private readonly CultureInfo _cultureInfo;
    private readonly ILogger _logger;
    private readonly ColumnMappingCollection _columnMappingCollection;

    public Mapping Mapping { get; }

    public XmlDestinationWriter(Mapping mapping, System.Xml.XmlWriter xmlWriter, bool skipTroublesomeRows, CultureInfo cultureInfo, ILogger logger)
    {
        _logger = logger;
        Mapping = mapping;
        _xmlWriter = xmlWriter;
        _skipTroublesomeRows = skipTroublesomeRows;
        _cultureInfo = cultureInfo;
        _columnMappingCollection = Mapping.GetColumnMappings();
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
                if (mapping.Active)
                {
                    if (p.ContainsKey(mapping.SourceColumn.Name))
                    {
                        _xmlWriter.WriteStartElement("column");
                        _xmlWriter.WriteAttributeString("columnName", mapping.DestinationColumn.Name);

                        if (mapping.HasScriptWithValue ||
                            (mapping.SourceColumn == null && mapping.ScriptType != ScriptType.None)) //in case when "None" selected as a SourceColumn                                
                        {
                            _xmlWriter.WriteCData(mapping.GetScriptValue());
                        }
                        else if (p[mapping.SourceColumn.Name] is DBNull || p[mapping.SourceColumn.Name] is null)
                        {
                            _xmlWriter.WriteAttributeString("isNull", "true");
                        }
                        else if (mapping.SourceColumn.Type == typeof(DateTime))
                        {
                            if (DateTime.TryParse(p[mapping.SourceColumn.Name].ToString(), out var theDateTime))
                            {
                                _xmlWriter.WriteCData(theDateTime.ToString("dd-MM-yyyy HH:mm:ss:fff"));
                            }
                            else
                            {
                                _xmlWriter.WriteCData(DateTime.MinValue.ToString("dd-MM-yyyy HH:mm:ss:fff"));
                            }
                        }
                        else if (_cultureInfo != null && (mapping.SourceColumn.Type == typeof(int) ||
                                mapping.SourceColumn.Type == typeof(decimal) ||
                                mapping.SourceColumn.Type == typeof(double) ||
                                mapping.SourceColumn.Type == typeof(float)))
                        {
                            string value = ValueFormatter.GetFormattedValue(p[mapping.SourceColumn.Name], _cultureInfo,
                                mapping.ScriptType, mapping.ScriptValue);
                            _xmlWriter.WriteCData(value);
                        }
                        else
                        {
                            switch (mapping.ScriptType)
                            {
                                case ScriptType.Append:
                                    _xmlWriter.WriteCData(p[mapping.SourceColumn.Name].ToString() + mapping.ScriptValue);
                                    break;
                                case ScriptType.Prepend:
                                    _xmlWriter.WriteCData(mapping.ScriptValue + p[mapping.SourceColumn.Name].ToString());
                                    break;
                                case ScriptType.None:
                                    _xmlWriter.WriteCData(p[mapping.SourceColumn.Name].ToString());
                                    break;
                            }
                        }
                        _xmlWriter.WriteEndElement();
                    }
                    else
                    {
                        throw new Exception(BaseDestinationWriter.GetRowValueNotFoundMessage(p, mapping.SourceColumn.Table.Name, mapping.SourceColumn.Name));
                    }
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
    /// Removes control characters and other non-UTF-8 characters
    /// </summary>
    /// <param name="inString">The string to process</param>
    /// <returns>A string with no control characters or entities above 0x00FD</returns>
    private static bool StringIsFreeOfTroublesomeCharacters(string inString)
    {
        if (inString == null) return true;
        return inString.All(ch => ((ch >= 0x0020 && ch <= 0xD7FF) || (ch >= 0xE000 && ch <= 0xFFFD) || ch == 0x0009 || ch == 0x000A || ch == 0x000D));
    }
}
