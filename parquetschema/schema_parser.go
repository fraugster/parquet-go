package parquetschema

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/fraugster/parquet-go/parquet"
)

type item struct {
	typ  itemType
	pos  pos
	val  string
	line int
}

type pos int

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case len(i.val) > 10:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q", i.val)
}

type itemType int

const (
	itemError itemType = iota
	itemEOF

	itemLeftParen
	itemRightParen
	itemLeftBrace
	itemRightBrace
	itemEqual
	itemSemicolon
	itemComma
	itemNumber
	itemIdentifier
	itemKeyword
	itemMessage
	itemRepeated
	itemOptional
	itemRequired
	itemGroup
)

func (i itemType) String() string {
	typeNames := map[itemType]string{
		itemError:      "error",
		itemEOF:        "EOF",
		itemLeftParen:  "(",
		itemRightParen: ")",
		itemLeftBrace:  "{",
		itemRightBrace: "}",
		itemEqual:      "=",
		itemSemicolon:  ";",
		itemComma:      ",",
		itemNumber:     "number",
		itemIdentifier: "identifier",
		itemKeyword:    "<keyword>",
		itemMessage:    "message",
		itemRepeated:   "repeated",
		itemOptional:   "optional",
		itemRequired:   "required",
		itemGroup:      "group",
	}

	n, ok := typeNames[i]
	if !ok {
		return fmt.Sprintf("<type:%d>", int(i))
	}
	return n
}

var key = map[string]itemType{
	"message":  itemMessage,
	"repeated": itemRepeated,
	"optional": itemOptional,
	"required": itemRequired,
	"group":    itemGroup,
}

const eof = -1

type stateFn func(*schemaLexer) stateFn

type schemaLexer struct {
	input     string
	pos       pos
	start     pos
	width     pos
	items     chan item
	line      int
	startLine int
}

func (l *schemaLexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.width = 0
		return eof
	}

	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = pos(w)
	l.pos += l.width
	if r == '\n' {
		l.line++
	}
	return r
}

func (l *schemaLexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *schemaLexer) backup() {
	l.pos -= l.width
	if l.width == 1 && l.input[l.pos] == '\n' {
		l.line--
	}
}

func (l *schemaLexer) ignore() {
	l.start = l.pos
	l.startLine = l.line
}

func (l *schemaLexer) emit(t itemType) {
	l.items <- item{t, l.start, l.input[l.start:l.pos], l.startLine}
	l.start = l.pos
	l.startLine = l.line
}

func (l *schemaLexer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.next()) {
	}
	l.backup()
}

func (l *schemaLexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, l.start, fmt.Sprintf(format, args...), l.startLine}
	return nil
}

func (l *schemaLexer) nextItem() item {
	return <-l.items
}

func (l *schemaLexer) drain() {
	for range l.items {
	}
}

func lex(input string) *schemaLexer {
	l := &schemaLexer{
		input:     input,
		items:     make(chan item),
		line:      1,
		startLine: 1,
	}

	go l.run()
	return l
}

func (l *schemaLexer) run() {
	for state := lexText; state != nil; {
		state = state(l)
	}
	close(l.items)
}

func lexText(l *schemaLexer) stateFn {
	switch r := l.next(); {
	case r == eof:
		l.emit(itemEOF)
		return nil
	case isSpace(r):
		return lexSpace
	case r == '(':
		l.emit(itemLeftParen)
	case r == ')':
		l.emit(itemRightParen)
	case r == '{':
		l.emit(itemLeftBrace)
	case r == '}':
		l.emit(itemRightBrace)
	case isDigit(r):
		return lexNumber
	case r == '=':
		l.emit(itemEqual)
	case r == ';':
		l.emit(itemSemicolon)
	case r == ',':
		l.emit(itemComma)
	case isAlpha(r):
		return lexIdentifier
	default:
		l.errorf("unknown start of token '%v'", r)
	}
	return lexText
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

func isDigit(r rune) bool {
	return unicode.IsDigit(r)
}

func isAlpha(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isAlphaNum(r rune) bool {
	return isAlpha(r) || isDigit(r)
}

func lexSpace(l *schemaLexer) stateFn {
	for isSpace(l.peek()) {
		l.next()
	}
	l.ignore()
	return lexText
}

func lexNumber(l *schemaLexer) stateFn {
	l.acceptRun("0123456789")
	l.emit(itemNumber)
	return lexText
}

func lexIdentifier(l *schemaLexer) stateFn {
loop:
	for {
		switch r := l.next(); {
		case isAlphaNum(r): // the = is there to accept it as part of the identifiers being read within type annotations.
			// absorb.
		default:
			l.backup()
			word := l.input[l.start:l.pos]
			switch {
			case key[word] > itemKeyword:
				l.emit(key[word])
			default:
				l.emit(itemIdentifier)
			}
			break loop
		}
	}
	return lexText
}

type schemaParser struct {
	l     *schemaLexer
	token item
	root  *ColumnDefinition
}

func newSchemaParser(text string) *schemaParser {
	return &schemaParser{
		l:    lex(text),
		root: &ColumnDefinition{SchemaElement: &parquet.SchemaElement{}},
	}
}

func (p *schemaParser) parse() (err error) {
	defer p.recover(&err)

	p.parseMessage()

	p.next()
	p.expect(itemEOF)

	p.validateLogicalTypes(p.root)

	return nil
}

func (p *schemaParser) recover(errp *error) {
	if e := recover(); e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		p.l.drain()
		*errp = e.(error)
	}
}

func (p *schemaParser) errorf(msg string, args ...interface{}) {
	msg = fmt.Sprintf("line %d: %s", p.token.line, msg)
	panic(fmt.Errorf(msg, args...))
}

func (p *schemaParser) expect(typ itemType) {
	if typ == itemIdentifier && p.token.typ > itemKeyword {
		return
	}

	if p.token.typ != typ {
		p.errorf("expected %s, got %s instead", typ, p.token)
	}
}

func (p *schemaParser) next() {
	p.token = p.l.nextItem()
}

func (p *schemaParser) parseMessage() {
	p.next()
	p.expect(itemMessage)

	p.next()
	p.expect(itemIdentifier)

	p.root.SchemaElement.Name = p.token.val

	// TODO: add support for logical type annotations as mentioned here:
	// https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/schema/MessageType.java#L65

	p.next()
	p.expect(itemLeftBrace)

	p.root.Children = p.parseMessageBody()
	for _, c := range p.root.Children {
		recursiveFix(c)
	}

	p.expect(itemRightBrace)
}

func recursiveFix(col *ColumnDefinition) {
	if nc := int32(len(col.Children)); nc > 0 {
		col.SchemaElement.NumChildren = &nc
	}

	for i := range col.Children {
		recursiveFix(col.Children[i])
	}
}

func (p *schemaParser) parseMessageBody() []*ColumnDefinition {
	var cols []*ColumnDefinition
	p.expect(itemLeftBrace)
	for {
		p.next()
		//log.Printf("token = %s", p.token)
		if p.token.typ == itemRightBrace {
			return cols
		}

		cols = append(cols, p.parseColumnDefinition())
	}
}

func (p *schemaParser) parseColumnDefinition() *ColumnDefinition {
	col := &ColumnDefinition{
		SchemaElement: &parquet.SchemaElement{},
	}

	switch p.token.typ {
	case itemRepeated:
		col.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED)
	case itemOptional:
		col.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	case itemRequired:
		col.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
	default:
		p.errorf("invalid field repetition type %q", p.token.val)
	}

	p.next()

	if p.token.typ == itemGroup {
		p.next()
		p.expect(itemIdentifier)
		col.SchemaElement.Name = p.token.val

		p.next()
		if p.token.typ == itemLeftParen {
			col.SchemaElement.ConvertedType = p.parseConvertedType()
			p.next()
		}

		col.Children = p.parseMessageBody()

		p.expect(itemRightBrace)
	} else {
		col.SchemaElement.Type = p.getTokenType()

		if col.SchemaElement.GetType() == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			p.next()
			p.expect(itemLeftParen)
			p.next()
			p.expect(itemNumber)

			i, err := strconv.ParseInt(p.token.val, 10, 32)
			if err != nil || i < 0 {
				p.errorf("invalid fixed_len_byte_array length %q", p.token.val)
			}

			byteArraySize := int32(i)

			col.SchemaElement.TypeLength = &byteArraySize

			p.next()
			p.expect(itemRightParen)
		}

		p.next()
		p.expect(itemIdentifier)
		col.SchemaElement.Name = p.token.val

		p.next()
		if p.token.typ == itemLeftParen {
			col.SchemaElement.LogicalType, col.SchemaElement.ConvertedType = p.parseLogicalOrConvertedType()
			if col.SchemaElement.LogicalType != nil && col.SchemaElement.LogicalType.IsSetDECIMAL() {
				col.SchemaElement.Scale = &col.SchemaElement.LogicalType.DECIMAL.Scale
				col.SchemaElement.Precision = &col.SchemaElement.LogicalType.DECIMAL.Precision
			}
			p.next()
		}

		if p.token.typ == itemEqual {
			col.SchemaElement.FieldID = p.parseFieldID()
			p.next()
		}

		p.expect(itemSemicolon)
	}

	return col
}

func (p *schemaParser) isValidType(typ string) {
	validTypes := []string{"binary", "float", "double", "boolean", "int32", "int64", "int96", "fixed_len_byte_array"}
	for _, vt := range validTypes {
		if vt == typ {
			return
		}
	}
	p.errorf("invalid type %q", typ)
}

func (p *schemaParser) getTokenType() *parquet.Type {
	p.isValidType(p.token.val)

	switch p.token.val {
	case "binary":
		return parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	case "float":
		return parquet.TypePtr(parquet.Type_FLOAT)
	case "double":
		return parquet.TypePtr(parquet.Type_DOUBLE)
	case "boolean":
		return parquet.TypePtr(parquet.Type_BOOLEAN)
	case "int32":
		return parquet.TypePtr(parquet.Type_INT32)
	case "int64":
		return parquet.TypePtr(parquet.Type_INT64)
	case "int96":
		return parquet.TypePtr(parquet.Type_INT96)
	case "fixed_len_byte_array":
		return parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
	default:
		p.errorf("unsupported type %q", p.token.val)
		return nil
	}
}

func (p *schemaParser) parseLogicalOrConvertedType() (*parquet.LogicalType, *parquet.ConvertedType) {
	p.expect(itemLeftParen)
	p.next()
	p.expect(itemIdentifier)

	typStr := p.token.val

	lt := parquet.NewLogicalType()
	var ct *parquet.ConvertedType

	switch strings.ToUpper(typStr) {
	case "STRING":
		lt.STRING = parquet.NewStringType()
		ct = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
	case "DATE":
		lt.DATE = parquet.NewDateType()
		ct = parquet.ConvertedTypePtr(parquet.ConvertedType_DATE)
	case "TIMESTAMP":
		lt.TIMESTAMP = parquet.NewTimestampType()
		p.next()
		p.expect(itemLeftParen)

		p.next()
		p.expect(itemIdentifier)

		lt.TIMESTAMP.Unit = parquet.NewTimeUnit()
		switch p.token.val {
		case "MILLIS":
			lt.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS)
		case "MICROS":
			lt.TIMESTAMP.Unit.MICROS = parquet.NewMicroSeconds()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS)
		case "NANOS":
			lt.TIMESTAMP.Unit.NANOS = parquet.NewNanoSeconds()
		default:
			p.errorf("unknown unit annotation %q for TIMESTAMP", p.token.val)
		}

		p.next()
		p.expect(itemComma)

		p.next()
		p.expect(itemIdentifier)

		switch p.token.val {
		case "true", "false":
			lt.TIMESTAMP.IsAdjustedToUTC, _ = strconv.ParseBool(p.token.val)
		default:
			p.errorf("invalid isAdjustedToUTC annotation %q for TIMESTAMP", p.token.val)
		}

		p.next()
		p.expect(itemRightParen)
	case "TIME":
		lt.TIME = parquet.NewTimeType()
		p.next()
		p.expect(itemLeftParen)

		p.next()
		p.expect(itemIdentifier)

		lt.TIME.Unit = parquet.NewTimeUnit()
		switch p.token.val {
		case "MILLIS":
			lt.TIME.Unit.MILLIS = parquet.NewMilliSeconds()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS)
		case "MICROS":
			lt.TIME.Unit.MICROS = parquet.NewMicroSeconds()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)
		case "NANOS":
			lt.TIME.Unit.NANOS = parquet.NewNanoSeconds()
		default:
			p.errorf("unknown unit annotation %q for TIME", p.token.val)
		}

		p.next()
		p.expect(itemComma)

		p.next()
		p.expect(itemIdentifier)

		switch p.token.val {
		case "true", "false":
			lt.TIME.IsAdjustedToUTC, _ = strconv.ParseBool(p.token.val)
		default:
			p.errorf("invalid isAdjustedToUTC annotation %q for TIME", p.token.val)
		}

		p.next()
		p.expect(itemRightParen)
	case "INT":
		lt.INTEGER = parquet.NewIntType()
		p.next()
		p.expect(itemLeftParen)

		p.next()
		p.expect(itemNumber)

		bitWidth, _ := strconv.ParseInt(p.token.val, 10, 64)
		if bitWidth != 8 && bitWidth != 16 && bitWidth != 32 && bitWidth != 64 {
			p.errorf("INT: unsupported bitwidth %d", bitWidth)
		}

		lt.INTEGER.BitWidth = int8(bitWidth)

		p.next()
		p.expect(itemComma)

		p.next()
		p.expect(itemIdentifier)
		switch p.token.val {
		case "true", "false":
			lt.INTEGER.IsSigned, _ = strconv.ParseBool(p.token.val)
		default:
			p.errorf("invalid isSigned annotation %q for INT", p.token.val)
		}

		p.next()
		p.expect(itemRightParen)

		convertedTypeStr := fmt.Sprintf("INT_%d", bitWidth)
		if !lt.INTEGER.IsSigned {
			convertedTypeStr = "U" + convertedTypeStr
		}

		convertedType, err := parquet.ConvertedTypeFromString(convertedTypeStr)
		if err != nil {
			p.errorf("couldn't convert INT(%d, %t) annotation to converted type %s: %v", bitWidth, lt.INTEGER.IsSigned, convertedTypeStr, err)
		}
		ct = parquet.ConvertedTypePtr(convertedType)
	case "UUID":
		lt.UUID = parquet.NewUUIDType()
	case "ENUM":
		lt.ENUM = parquet.NewEnumType()
		ct = parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM)
	case "JSON":
		lt.JSON = parquet.NewJsonType()
		ct = parquet.ConvertedTypePtr(parquet.ConvertedType_JSON)
	case "BSON":
		lt.BSON = parquet.NewBsonType()
		ct = parquet.ConvertedTypePtr(parquet.ConvertedType_BSON)
	case "DECIMAL":
		lt.DECIMAL = parquet.NewDecimalType()
		p.next()
		p.expect(itemLeftParen)

		p.next()
		p.expect(itemNumber)

		prec, _ := strconv.ParseInt(p.token.val, 10, 64)
		lt.DECIMAL.Precision = int32(prec)

		p.next()
		p.expect(itemComma)

		p.next()
		p.expect(itemNumber)

		scale, _ := strconv.ParseInt(p.token.val, 10, 64)
		lt.DECIMAL.Scale = int32(scale)

		p.next()
		p.expect(itemRightParen)
	default:
		convertedType, err := parquet.ConvertedTypeFromString(strings.ToUpper(typStr))
		if err != nil {
			p.errorf("unsupported logical type or converted type %q", typStr)
		}
		lt = nil
		ct = &convertedType
	}

	p.next()
	p.expect(itemRightParen)

	return lt, ct
}

func (p *schemaParser) parseConvertedType() *parquet.ConvertedType {
	p.expect(itemLeftParen)
	p.next()
	p.expect(itemIdentifier)

	typStr := p.token.val

	// TODO: is this correct? compare with Java implementation.
	convertedType, err := parquet.ConvertedTypeFromString(typStr)
	if err != nil {
		p.errorf("invalid converted type %q", typStr)
	}

	p.next()
	p.expect(itemRightParen)

	return parquet.ConvertedTypePtr(convertedType)
}

func (p *schemaParser) parseFieldID() *int32 {
	p.expect(itemEqual)
	p.next()
	p.expect(itemNumber)

	i, err := strconv.ParseInt(p.token.val, 10, 32)
	if err != nil {
		p.errorf("couldn't parse field ID %q: %v", p.token.val, err)
	}

	i32 := int32(i)

	return &i32
}

func (p *schemaParser) validateLogicalTypes(col *ColumnDefinition) {
	if col.SchemaElement != nil && (col.SchemaElement.LogicalType != nil || col.SchemaElement.ConvertedType != nil) {
		switch {
		case (col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetLIST()) || col.SchemaElement.GetConvertedType() == parquet.ConvertedType_LIST:
			// TODO: add support for backward compatibility:
			// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
			if col.SchemaElement.Type != nil {
				p.errorf("field %s is not a group but annotated as LIST", col.SchemaElement.Name)
			}
			if rep := col.SchemaElement.GetRepetitionType(); rep != parquet.FieldRepetitionType_OPTIONAL && rep != parquet.FieldRepetitionType_REQUIRED {
				p.errorf("field %s is a LIST but has repetition type %s", col.SchemaElement.Name, rep)
			}
			if len(col.Children) != 1 {
				p.errorf("field %s is a LIST but has %d children", len(col.Children))
			}
			if col.Children[0].SchemaElement.Type != nil || col.Children[0].SchemaElement.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED {
				p.errorf("field %s is a LIST but its child is not a repeated group", col.SchemaElement.Name)
			}
			if col.Children[0].SchemaElement.Name != "list" {
				p.errorf("field %s is a LIST but its child is not named \"list\"", col.SchemaElement.Name)
			}
			if len(col.Children[0].Children) != 1 {
				p.errorf("field %s.list has %d children", col.SchemaElement.Name)
			}
			if col.Children[0].Children[0].SchemaElement.Name != "element" {
				p.errorf("%s.list has a child but it's called %q, not \"element\"", col.SchemaElement.Name, col.Children[0].Children[0].SchemaElement.Name)
			}
			if rep := col.Children[0].Children[0].SchemaElement.GetRepetitionType(); rep != parquet.FieldRepetitionType_OPTIONAL && rep != parquet.FieldRepetitionType_REQUIRED {
				p.errorf("%s.list.element has disallowed repetition type %s", col.SchemaElement.Name, rep)
			}
		case (col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetMAP()) || col.SchemaElement.GetConvertedType() == parquet.ConvertedType_MAP:
			if col.SchemaElement.Type != nil {
				p.errorf("field %s is not a group but annotated as MAP", col.SchemaElement.Name)
			}
			if len(col.Children) != 1 {
				p.errorf("field %s is a MAP but has %d children", len(col.Children))
			}
			if col.Children[0].SchemaElement.Type != nil || col.Children[0].SchemaElement.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED {
				p.errorf("filed %s is a MAP but its child is not a repeated group", col.SchemaElement.Name)
			}
			if col.Children[0].SchemaElement.Name != "key_value" {
				p.errorf("field %s is a MAP but its child is not named \"key_value\"", col.SchemaElement.Name)
			}
			foundKey := false
			foundValue := false
			for _, c := range col.Children[0].Children {
				switch c.SchemaElement.Name {
				case "key":
					if c.SchemaElement.GetRepetitionType() != parquet.FieldRepetitionType_REQUIRED {
						p.errorf("field %s.key_value.key is not of repetition type \"required\"", col.SchemaElement.Name)
					}
					foundKey = true
				case "value":
					foundValue = true
					// nothing else to check.
				default:
					p.errorf("field %[1]s is a MAP so %[1]s.key_value.%[2]s is not allowed", col.SchemaElement.Name, c.SchemaElement.Name)
				}
			}
			if !foundKey {
				p.errorf("field %[1]s is missing %[1]s.key_value.key", col.SchemaElement.Name)
			}
			if !foundValue {
				p.errorf("field %[1]s is missing %[1]s.key_value.value", col.SchemaElement.Name)
			}
		case (col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetDATE()) || col.SchemaElement.GetConvertedType() == parquet.ConvertedType_DATE:
			if col.SchemaElement.GetType() != parquet.Type_INT32 {
				p.errorf("field %[1]s is annotated as DATE but is not an int32", col.SchemaElement.Name)
			}
		case col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetTIMESTAMP():
			if col.SchemaElement.GetType() != parquet.Type_INT64 && col.SchemaElement.GetType() != parquet.Type_INT96 {
				p.errorf("field %s is annotated as TIMESTAMP but is not an int64/int96", col.SchemaElement.Name)
			}
		case col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetTIME():
			t := col.SchemaElement.GetLogicalType().TIME
			switch {
			case t.Unit.IsSetNANOS():
				if col.SchemaElement.GetType() != parquet.Type_INT64 {
					p.errorf("field %s is annotated as TIME(NANOS, %t) but is not an int64", col.SchemaElement.Name, t.IsAdjustedToUTC)
				}
			case t.Unit.IsSetMICROS():
				if col.SchemaElement.GetType() != parquet.Type_INT64 {
					p.errorf("field %s is annotated as TIME(MICROS, %t) but is not an int64", col.SchemaElement.Name, t.IsAdjustedToUTC)
				}
			case t.Unit.IsSetMILLIS():
				if col.SchemaElement.GetType() != parquet.Type_INT32 {
					p.errorf("field %s is annotated as TIME(MILLIS, %t) but is not an int32", col.SchemaElement.Name, t.IsAdjustedToUTC)
				}
			}
		case col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetUUID():
			if col.SchemaElement.GetType() != parquet.Type_FIXED_LEN_BYTE_ARRAY || col.SchemaElement.GetTypeLength() != 16 {
				p.errorf("field %s is annotated as UUID but is not a fixed_len_byte_array(16)", col.SchemaElement.Name)
			}
		case col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetENUM():
			if col.SchemaElement.GetType() != parquet.Type_BYTE_ARRAY {
				p.errorf("field %s is annotated as ENUM but is not a binary", col.SchemaElement.Name)
			}
		case col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetJSON():
			if col.SchemaElement.GetType() != parquet.Type_BYTE_ARRAY {
				p.errorf("field %s is annotated as JSON but is not a binary", col.SchemaElement.Name)
			}
		case col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetBSON():
			if col.SchemaElement.GetType() != parquet.Type_BYTE_ARRAY {
				p.errorf("field %s is annotated as BSON but is not a binary", col.SchemaElement.Name)
			}
		case col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetDECIMAL():
			dec := col.SchemaElement.GetLogicalType().DECIMAL
			switch col.SchemaElement.GetType() {
			case parquet.Type_INT32:
				if dec.Precision < 1 || dec.Precision > 9 {
					p.errorf("field %s is int32 and annotated as DECIMAL but precision %d is out of bounds; needs to be 1 <= precision <= 9", col.SchemaElement.Name, dec.Precision)
				}
			case parquet.Type_INT64:
				if dec.Precision < 1 || dec.Precision > 18 {
					p.errorf("field %s is int64 and annotated as DECIMAL but precision %d is out of bounds; needs to be 1 <= precision <= 18", col.SchemaElement.Name, dec.Precision)
				}
			case parquet.Type_FIXED_LEN_BYTE_ARRAY:
				n := *col.SchemaElement.TypeLength
				maxDigits := int32(math.Floor(math.Log10(math.Exp2(8*float64(n)-1)) - 1))
				if dec.Precision < 1 || dec.Precision > maxDigits {
					p.errorf("field %s is fixed_len_byte_array(%d) and annotated as DECIMAL but precision %d is out of bounds; needs to be 1 <= precision <= %d", col.SchemaElement.Name, n, dec.Precision, maxDigits)
				}
			case parquet.Type_BYTE_ARRAY:
				if dec.Precision < 1 {
					p.errorf("field %s is int64 and annotated as DECIMAL but precision %d is out of bounds; needs to be 1 <= precision", col.SchemaElement.Name, dec.Precision)
				}
			default:
				p.errorf("field %s is annotated as DECIMAL but type %s is unsupported", col.SchemaElement.Name, col.SchemaElement.GetType().String())
			}
		case col.SchemaElement.LogicalType != nil && col.SchemaElement.GetLogicalType().IsSetINTEGER():
			bitWidth := col.SchemaElement.LogicalType.INTEGER.BitWidth
			isSigned := col.SchemaElement.LogicalType.INTEGER.IsSigned
			switch bitWidth {
			case 8, 16, 32:
				if col.SchemaElement.GetType() != parquet.Type_INT32 {
					p.errorf("field %s is annotated as INT(%d, %t) but element type is %s", col.SchemaElement.Name, bitWidth, isSigned, col.SchemaElement.GetType().String())
				}
			case 64:
				if col.SchemaElement.GetType() != parquet.Type_INT64 {
					p.errorf("field %s is annotated as INT(%d, %t) but element type is %s", col.SchemaElement.Name, bitWidth, isSigned, col.SchemaElement.GetType().String())
				}
			default:
				p.errorf("invalid bitWidth %d", bitWidth)
			}
		case col.SchemaElement.ConvertedType != nil && col.SchemaElement.GetConvertedType() == parquet.ConvertedType_UTF8:
			if col.SchemaElement.GetType() != parquet.Type_BYTE_ARRAY {
				p.errorf("field %s is annotated as UTF8 but element type is %s, not binary", col.SchemaElement.Name, col.SchemaElement.GetType().String())
			}
		case col.SchemaElement.ConvertedType != nil && col.SchemaElement.GetConvertedType() == parquet.ConvertedType_TIME_MILLIS:
			if col.SchemaElement.GetType() != parquet.Type_INT32 {
				p.errorf("field %s is annotated as TIME_MILLIS but element type is %s, not int32", col.SchemaElement.Name, col.SchemaElement.GetType().String())
			}
		case col.SchemaElement.ConvertedType != nil && col.SchemaElement.GetConvertedType() == parquet.ConvertedType_TIME_MICROS:
			if col.SchemaElement.GetType() != parquet.Type_INT64 {
				p.errorf("field %s is annotated as TIME_MICROS but element type is %s, not int64", col.SchemaElement.Name, col.SchemaElement.GetType().String())
			}
		case col.SchemaElement.ConvertedType != nil && col.SchemaElement.GetConvertedType() == parquet.ConvertedType_TIMESTAMP_MILLIS:
			if col.SchemaElement.GetType() != parquet.Type_INT64 {
				p.errorf("field %s is annotated as TIMESTAMP_MILLIS but element type is %s, not int64", col.SchemaElement.Name, col.SchemaElement.GetType().String())
			}
		case col.SchemaElement.ConvertedType != nil && col.SchemaElement.GetConvertedType() == parquet.ConvertedType_TIMESTAMP_MICROS:
			if col.SchemaElement.GetType() != parquet.Type_INT64 {
				p.errorf("field %s is annotated as TIMESTAMP_MICROS but element type is %s, not int64", col.SchemaElement.Name, col.SchemaElement.GetType().String())
			}
		case col.SchemaElement.ConvertedType != nil &&
			(col.SchemaElement.GetConvertedType() == parquet.ConvertedType_UINT_8 ||
				col.SchemaElement.GetConvertedType() == parquet.ConvertedType_UINT_16 ||
				col.SchemaElement.GetConvertedType() == parquet.ConvertedType_UINT_32 ||
				col.SchemaElement.GetConvertedType() == parquet.ConvertedType_INT_8 ||
				col.SchemaElement.GetConvertedType() == parquet.ConvertedType_INT_16 ||
				col.SchemaElement.GetConvertedType() == parquet.ConvertedType_INT_32):
			if col.SchemaElement.GetType() != parquet.Type_INT32 {
				p.errorf("field %s is annotated as %s but element type is %s, not int32", col.SchemaElement.Name, col.SchemaElement.GetConvertedType().String(), col.SchemaElement.GetType().String())
			}
		case col.SchemaElement.ConvertedType != nil &&
			(col.SchemaElement.GetConvertedType() == parquet.ConvertedType_UINT_64 ||
				col.SchemaElement.GetConvertedType() == parquet.ConvertedType_INT_64):
			if col.SchemaElement.GetType() != parquet.Type_INT64 {
				p.errorf("field %s is annotated as %s but element type is %s, not int64", col.SchemaElement.Name, col.SchemaElement.GetConvertedType().String(), col.SchemaElement.GetType().String())
			}
		case col.SchemaElement.ConvertedType != nil && col.SchemaElement.GetConvertedType() == parquet.ConvertedType_INTERVAL:
			if col.SchemaElement.GetType() != parquet.Type_FIXED_LEN_BYTE_ARRAY || col.SchemaElement.GetTypeLength() != 12 {
				p.errorf("field %s is annotated as INTERVAL but element type is %s, not fixed_len_byte_array(12)", col.SchemaElement.Name, col.SchemaElement.GetType().String())
			}
		}
	}

	for _, c := range col.Children {
		p.validateLogicalTypes(c)
	}
}
