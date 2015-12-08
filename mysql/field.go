// Copyright 2015 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mysql

import (
	"encoding/binary"

	"github.com/lostz/Aegis/utils"
)

type FieldData []byte

type Field struct {
	Data         FieldData
	Schema       []byte
	Table        []byte
	OrgTable     []byte
	Name         []byte
	OrgName      []byte
	Charset      uint16
	ColumnLength uint32
	Type         uint8
	Flag         uint16
	Decimal      uint8

	DefaultValueLength uint64
	DefaultValue       []byte
}

func (p FieldData) Parse() (f *Field, err error) {
	f = new(Field)

	f.Data = p

	var n int
	pos := 0
	//skip catelog, always def
	n, err = utils.SkipLengthEnodedString(p)
	if err != nil {
		return
	}
	pos += n

	//schema
	f.Schema, _, n, err = utils.LengthEnodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//table
	f.Table, _, n, err = utils.LengthEnodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//org_table
	f.OrgTable, _, n, err = utils.LengthEnodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//name
	f.Name, _, n, err = utils.LengthEnodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//org_name
	f.OrgName, _, n, err = utils.LengthEnodedString(p[pos:])
	if err != nil {
		return
	}
	pos += n

	//skip oc
	pos += 1

	//charset
	f.Charset = binary.LittleEndian.Uint16(p[pos:])
	pos += 2

	//column length
	f.ColumnLength = binary.LittleEndian.Uint32(p[pos:])
	pos += 4

	//type
	f.Type = p[pos]
	pos++

	//flag
	f.Flag = binary.LittleEndian.Uint16(p[pos:])
	pos += 2

	//decimals 1
	f.Decimal = p[pos]
	pos++

	//filter [0x00][0x00]
	pos += 2

	f.DefaultValue = nil
	//if more data, command was field list
	if len(p) > pos {
		//length of default value lenenc-int
		f.DefaultValueLength, _, n = LengthEncodedInt(p[pos:])
		pos += n

		if pos+int(f.DefaultValueLength) > len(p) {
			err = ErrMalformPacket
			return
		}

		//default value string[$len]
		f.DefaultValue = p[pos:(pos + int(f.DefaultValueLength))]
	}

	return
}

func (f *Field) Dump() []byte {
	if f.Data != nil {
		return []byte(f.Data)
	}

	l := len(f.Schema) + len(f.Table) + len(f.OrgTable) + len(f.Name) + len(f.OrgName) + len(f.DefaultValue) + 48

	data := make([]byte, 0, l)

	data = append(data, utils.PutLengthEncodedString([]byte("def"))...)

	data = append(data, utils.PutLengthEncodedString(f.Schema)...)

	data = append(data, utils.PutLengthEncodedString(f.Table)...)
	data = append(data, utils.PutLengthEncodedString(f.OrgTable)...)

	data = append(data, utils.PutLengthEncodedString(f.Name)...)
	data = append(data, utils.PutLengthEncodedString(f.OrgName)...)

	data = append(data, 0x0c)

	data = append(data, utils.Uint16ToBytes(f.Charset)...)
	data = append(data, utils.Uint32ToBytes(f.ColumnLength)...)
	data = append(data, f.Type)
	data = append(data, utils.Uint16ToBytes(f.Flag)...)
	data = append(data, f.Decimal)
	data = append(data, 0, 0)

	if f.DefaultValue != nil {
		data = append(data, utils.Uint64ToBytes(f.DefaultValueLength)...)
		data = append(data, f.DefaultValue...)
	}

	return data
}
