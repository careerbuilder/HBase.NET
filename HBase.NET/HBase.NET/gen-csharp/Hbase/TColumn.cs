/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Thrift;
using Thrift.Collections;
using System.Runtime.Serialization;
using Thrift.Protocol;
using Thrift.Transport;

namespace Hbase
{

  /// <summary>
  /// Holds column name and the cell.
  /// </summary>
  #if !SILVERLIGHT
  [Serializable]
  #endif
  public partial class TColumn : TBase
  {
    private byte[] _columnName;
    private TCell _cell;

    public byte[] ColumnName
    {
      get
      {
        return _columnName;
      }
      set
      {
        __isset.columnName = true;
        this._columnName = value;
      }
    }

    public TCell Cell
    {
      get
      {
        return _cell;
      }
      set
      {
        __isset.cell = true;
        this._cell = value;
      }
    }


    public Isset __isset;
    #if !SILVERLIGHT
    [Serializable]
    #endif
    public struct Isset {
      public bool columnName;
      public bool cell;
    }

    public TColumn() {
    }

    public void Read (TProtocol iprot)
    {
      TField field;
      iprot.ReadStructBegin();
      while (true)
      {
        field = iprot.ReadFieldBegin();
        if (field.Type == TType.Stop) { 
          break;
        }
        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.String) {
              ColumnName = iprot.ReadBinary();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 2:
            if (field.Type == TType.Struct) {
              Cell = new TCell();
              Cell.Read(iprot);
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          default: 
            TProtocolUtil.Skip(iprot, field.Type);
            break;
        }
        iprot.ReadFieldEnd();
      }
      iprot.ReadStructEnd();
    }

    public void Write(TProtocol oprot) {
      TStruct struc = new TStruct("TColumn");
      oprot.WriteStructBegin(struc);
      TField field = new TField();
      if (ColumnName != null && __isset.columnName) {
        field.Name = "columnName";
        field.Type = TType.String;
        field.ID = 1;
        oprot.WriteFieldBegin(field);
        oprot.WriteBinary(ColumnName);
        oprot.WriteFieldEnd();
      }
      if (Cell != null && __isset.cell) {
        field.Name = "cell";
        field.Type = TType.Struct;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        Cell.Write(oprot);
        oprot.WriteFieldEnd();
      }
      oprot.WriteFieldStop();
      oprot.WriteStructEnd();
    }

    public override string ToString() {
      StringBuilder __sb = new StringBuilder("TColumn(");
      bool __first = true;
      if (ColumnName != null && __isset.columnName) {
        if(!__first) { __sb.Append(", "); }
        __first = false;
        __sb.Append("ColumnName: ");
        __sb.Append(ColumnName);
      }
      if (Cell != null && __isset.cell) {
        if(!__first) { __sb.Append(", "); }
        __first = false;
        __sb.Append("Cell: ");
        __sb.Append(Cell== null ? "<null>" : Cell.ToString());
      }
      __sb.Append(")");
      return __sb.ToString();
    }

  }

}
