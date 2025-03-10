﻿using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace Dynamicweb.DataIntegration.Providers.XmlProvider;

class XmlSourceReader : ISourceReader
{
    private XmlReader _xmlReader;
    private readonly Mapping _mapping;
    Dictionary<string, object> _nextRow;
    private readonly XmlProvider _provider;
    private readonly string _decimalSeparator;
    private readonly bool _autoDetectDecimalSeparator;
    private readonly ColumnMappingCollection _columnMappings;

    public XmlSourceReader(XmlProvider provider, Mapping mapping, string decimalSeparator, bool autoDetectDecimalSeparator)
    {
        _provider = provider;
        _mapping = mapping;
        _decimalSeparator = decimalSeparator;
        _autoDetectDecimalSeparator = autoDetectDecimalSeparator;
        _xmlReader = _provider.GetXmlReader(_mapping);
        _columnMappings = _mapping.GetColumnMappings();
    }

    public bool IsDone()
    {
        if (_xmlReader != null)
        {
            do
            {
                while (_xmlReader.Read())
                {
                    if (_xmlReader.NodeType == XmlNodeType.Element && _xmlReader.Name == "item")
                    {
                        break;
                    }
                    else if ((_xmlReader.NodeType == XmlNodeType.EndElement && (_xmlReader.Name == "table" || _xmlReader.Name == "tables")) || (_xmlReader.NodeType == XmlNodeType.Element && _xmlReader.Name == "table"))
                    {
                        _xmlReader.Close();
                        return true;
                    }

                }

                SetNextRow();

                ReplaceDecimalSeparator();
            }
            while (!RowMatchesConditions());

            return false;
        }
        else
        {
            return true;
        }
    }

    private bool RowMatchesConditions()
    {
        return _mapping.Conditionals?.CheckConditionals(_nextRow) ?? true;
    }

    public Dictionary<string, object> GetNext()
    {
        return _nextRow;
    }

    private void SetNextRow()
    {
        _nextRow = new Dictionary<string, object>();
        try
        {
            string itemName = _xmlReader.Name;
            string key = "";
            object value = "";

            while (_xmlReader.Read() && !(_xmlReader.NodeType == XmlNodeType.EndElement && _xmlReader.Name == itemName))
            {
                if (_xmlReader.NodeType == XmlNodeType.Element)
                {
                    bool isnull = false;
                    for (int i = 0; i < _xmlReader.AttributeCount; i++)
                    {
                        _xmlReader.MoveToAttribute(i);
                        if (_xmlReader.Name == "isNull" && _xmlReader.Value == "true")
                        {

                            value = DBNull.Value;
                            isnull = true;
                        }
                        if (_xmlReader.Name == "columnName")
                            key = _xmlReader.Value;


                    }
                    _xmlReader.MoveToElement();
                    if (!isnull)
                    {
                        if (_xmlReader.IsEmptyElement)
                        {
                            _nextRow.Add(key, "");
                            value = "";
                        }
                    }
                    else
                    {
                        if (_xmlReader.IsEmptyElement)
                        {
                            _nextRow.Add(key, DBNull.Value);
                            value = "";
                        }
                    }
                }

                //an xmlnode can contain several CDATA nodes - the content of these will be concatanated.
                if (_xmlReader.NodeType == XmlNodeType.CDATA || _xmlReader.NodeType == XmlNodeType.Text)
                {
                    {
                        if (value != DBNull.Value)
                            value = value + _xmlReader.Value;
                    }
                }
                if (_xmlReader.NodeType == XmlNodeType.EndElement)
                {
                    _nextRow.Add(key, value);
                    value = "";
                }
            }
            foreach (ColumnMapping cm in _columnMappings.Where(cm => cm != null && cm.Active && cm.SourceColumn != null))
            {
                if (!_nextRow.ContainsKey(cm.SourceColumn.Name))
                {
                    _nextRow.Add(cm.SourceColumn.Name, DBNull.Value);
                }
            }
        }
        catch (Exception ex)
        {
            string gottenResult = _nextRow.Aggregate("", (current, obj) => current + obj.Value.ToString() + ", ");
            if (gottenResult != "")
            {
                gottenResult = gottenResult.Substring(0, gottenResult.Length - 2);
            }
            throw new Exception("Read from file failed. Partial read from table '" + _mapping.SourceTable.Name + "': " + gottenResult + ".", ex);
        }
    }

    private void ReplaceDecimalSeparator()
    {
        if (_autoDetectDecimalSeparator || !string.IsNullOrEmpty(_decimalSeparator))
        {
            foreach (ColumnMapping cm in _columnMappings)
            {
                if (cm.SourceColumn != null && _nextRow.ContainsKey(cm.SourceColumn.Name) && _nextRow[cm.SourceColumn.Name] != DBNull.Value && !cm.HasScriptWithValue &&
                    cm.DestinationColumn != null && (cm.DestinationColumn.Type == typeof(double) || cm.DestinationColumn.Type == typeof(float)))
                {
                    string value = _nextRow[cm.SourceColumn.Name].ToString();
                    if (!string.IsNullOrEmpty(value))
                    {
                        if (_autoDetectDecimalSeparator)
                        {
                            _nextRow[cm.SourceColumn.Name] = Converter.ToDouble(value).ToString();
                        }
                        else
                        {
                            if (_decimalSeparator != System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator)
                            {
                                value =
                                    value.Replace(
                                        System.Globalization.CultureInfo.CurrentCulture.NumberFormat.
                                            NumberDecimalSeparator, "");
                                _nextRow[cm.SourceColumn.Name] = value.Replace(_decimalSeparator, System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                            }
                        }
                    }
                }
            }
        }
    }

    public void Dispose()
    {
        if (_xmlReader != null)
        {
            _xmlReader.Close();
        }
    }
}
