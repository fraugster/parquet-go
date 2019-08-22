package go_parquet

import (
	"fmt"
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
	l.line += strings.Count(l.input[l.start:l.pos], "\n")
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
	case r == '\n': // ignore newlines
		return lexText
	default:
		l.errorf("unknown start of token '%v'", r)
	}
	return lexText
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t'
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
	root  *column
}

func newSchemaParser(text string) *schemaParser {
	return &schemaParser{
		l:    lex(text),
		root: &column{},
	}
}

func (p *schemaParser) parse() (err error) {
	defer p.recover(&err)

	p.parseMessage()

	p.next()
	p.expect(itemEOF)

	for _, c := range p.root.children {
		fixFlatName("", c)
	}

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
	if p.token.typ != typ {
		//log.Printf("expected %s, got %s instead", typ, p.token)
		p.errorf("expected %s, got %s instead", typ, p.token)
	}
	//log.Printf("expect %s successful, token = %v", typ, p.token)
}

func (p *schemaParser) next() {
	p.token = p.l.nextItem()
	//log.Printf("next token: %s", p.token)
}

func (p *schemaParser) parseMessage() {
	p.next()
	p.expect(itemMessage)

	p.next()
	p.expect(itemIdentifier)

	p.root.name = p.token.val

	// TODO: add support for logical type annotations as mentioned here:
	// https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/schema/MessageType.java#L65

	p.next()
	p.expect(itemLeftBrace)

	p.root.children = p.parseMessageBody()

	p.expect(itemRightBrace)
}

func (p *schemaParser) parseMessageBody() []*column {
	var cols []*column
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

func (p *schemaParser) parseColumnDefinition() *column {
	col := &column{
		element: &parquet.SchemaElement{},
	}

	switch p.token.typ {
	case itemRepeated:
		col.rep = parquet.FieldRepetitionType_REPEATED
	case itemOptional:
		col.rep = parquet.FieldRepetitionType_OPTIONAL
	case itemRequired:
		col.rep = parquet.FieldRepetitionType_REQUIRED
	default:
		p.errorf("invalid field repetition type %q", p.token.val)
	}

	p.next()
	if p.token.typ == itemGroup {

		p.next()
		p.expect(itemIdentifier)
		col.name = p.token.val

		p.next()
		if p.token.typ == itemLeftParen {
			col.element.ConvertedType = p.parseConvertedType()
			p.next()
		}

		col.children = p.parseMessageBody()
		col.element.NumChildren = int32Ptr(int32(len(col.children)))

		p.expect(itemRightBrace)
	} else {
		col.element.Type = p.getTokenType()

		if col.element.GetType() == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			p.next()
			p.expect(itemLeftParen)
			p.next()
			p.expect(itemNumber)

			i, err := strconv.ParseInt(p.token.val, 10, 32)
			if err != nil || i < 0 {
				p.errorf("invalid fixed_len_byte_array length %q", p.token.val)
			}

			byteArraySize := int32(i)

			col.element.TypeLength = &byteArraySize

			p.next()
			p.expect(itemRightParen)
		}

		p.next()
		p.expect(itemIdentifier)
		col.name = p.token.val

		p.next()
		if p.token.typ == itemLeftParen {
			col.element.LogicalType = p.parseLogicalType()
			p.next()
		}

		if p.token.typ == itemEqual {
			col.element.FieldID = p.parseFieldID()
			p.next()
		}

		col.data = p.getColumnStore(col.element)
		col.data.reset(col.rep)

		p.expect(itemSemicolon)
	}

	col.element.Name = col.name
	col.element.RepetitionType = parquet.FieldRepetitionTypePtr(col.rep)

	return col
}

func int32Ptr(i int32) *int32 {
	return &i
}

func (p *schemaParser) isValidType(typ string) {
	validTypes := []string{"binary", "float", "double", "boolean", "int32", "int64", "int96", "fixed_len_byte_array"} // TODO: add more.
	for _, vt := range validTypes {
		if vt == typ {
			return
		}
	}
	p.errorf("invalid type %q", typ)
}

func (p *schemaParser) getTokenType() *parquet.Type {
	p.isValidType(p.token.val)

	// TODO: add support for fixed_len_byte_array; length is kept in logical type annotation
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

func (p *schemaParser) getColumnStore(elem *parquet.SchemaElement) *ColumnStore {
	if elem.Type == nil {
		return nil
	}

	var (
		colStore *ColumnStore
		err      error
	)

	typ := elem.GetType()

	switch typ {
	case parquet.Type_BYTE_ARRAY:
		colStore, err = NewByteArrayStore(parquet.Encoding_PLAIN, true)
	case parquet.Type_FLOAT:
		colStore, err = NewFloatStore(parquet.Encoding_PLAIN, true)
	case parquet.Type_DOUBLE:
		colStore, err = NewDoubleStore(parquet.Encoding_PLAIN, true)
	case parquet.Type_BOOLEAN:
		colStore, err = NewBooleanStore(parquet.Encoding_PLAIN)
	case parquet.Type_INT32:
		colStore, err = NewInt32Store(parquet.Encoding_PLAIN, true)
	case parquet.Type_INT64:
		colStore, err = NewInt64Store(parquet.Encoding_PLAIN, true)
	case parquet.Type_INT96:
		colStore, err = NewInt96Store(parquet.Encoding_PLAIN, true)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		colStore, err = NewFixedByteArrayStore(parquet.Encoding_PLAIN, true, int(elem.GetTypeLength()))
	default:
		p.errorf("unsupported type %q when creating column store", typ.String())
	}
	if err != nil {
		p.errorf("creating column store for type %q failed: %v", typ.String(), err)
	}

	return colStore
}

func (p *schemaParser) parseLogicalType() *parquet.LogicalType {
	p.expect(itemLeftParen)
	p.next()
	p.expect(itemIdentifier)

	typStr := p.token.val

	lt := parquet.NewLogicalType()

	p.next()

	var annotations []string

	if p.token.typ == itemLeftParen {
		p.next()
		for p.token.typ != itemRightParen {
			p.expect(itemIdentifier)
			annotations = append(annotations, p.token.val)
			p.next()
			switch p.token.typ {
			case itemComma, itemRightParen:
				// they are expected, nothing to do.
			default:
				p.errorf("unexpected %s", p.token)
			}
			p.next()
		}
	}

	p.expect(itemRightParen)

	switch strings.ToUpper(typStr) {
	case "STRING":
		lt.STRING = parquet.NewStringType()
	case "DATE":
		lt.DATE = parquet.NewDateType()
	case "TIMESTAMP":
		lt.TIMESTAMP = parquet.NewTimestampType()
		if len(annotations) != 2 {
			p.errorf("TIMESTAMP requires two annotations: unit, isAdjustedToUTC")
		}
		lt.TIMESTAMP.Unit = parquet.NewTimeUnit()
		switch annotations[0] {
		case "MILLIS":
			lt.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()
		case "MICROS":
			lt.TIMESTAMP.Unit.MICROS = parquet.NewMicroSeconds()
		case "NANOS":
			lt.TIMESTAMP.Unit.NANOS = parquet.NewNanoSeconds()
		default:
			p.errorf("unknown unit annotation %q for TIMESTAMP", annotations[0])
		}
		switch annotations[1] {
		case "true", "false":
			lt.TIMESTAMP.IsAdjustedToUTC, _ = strconv.ParseBool(annotations[1])
		default:
			p.errorf("invalid isAdjustedToUTC annotation %q for TIMESTAMP", annotations[1])
		}
	case "UUID":
		lt.UUID = parquet.NewUUIDType()
	case "ENUM":
		lt.ENUM = parquet.NewEnumType()
	case "JSON":
		lt.JSON = parquet.NewJsonType()
	default:
		p.errorf("unsupported logical type %q", typStr)
	}

	return lt
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

func fixFlatName(prefix string, col *column) {
	flatName := col.name
	if prefix != "" {
		flatName = prefix + "." + flatName
	}

	col.flatName = flatName

	for _, c := range col.children {
		fixFlatName(flatName, c)
	}
}

func (p *schemaParser) validateLogicalTypes(col *column) {
	if col.element != nil && (col.element.LogicalType != nil || col.element.ConvertedType != nil) {
		switch {
		case (col.element.LogicalType != nil && col.element.GetLogicalType().IsSetLIST()) || col.element.GetConvertedType() == parquet.ConvertedType_LIST:
			// TODO: add support for backward compatibility:
			// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
			if col.element.Type != nil {
				p.errorf("field %s is not a group but annotated as LIST", col.element.Name)
			}
			if rep := col.element.GetRepetitionType(); rep != parquet.FieldRepetitionType_OPTIONAL && rep != parquet.FieldRepetitionType_REQUIRED {
				p.errorf("field %s is a LIST but has repetition type %s", col.element.Name, rep)
			}
			if len(col.children) != 1 {
				p.errorf("field %s is a LIST but has %d children", len(col.children))
			}
			if col.children[0].element.Type != nil || col.children[0].element.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED {
				p.errorf("field %s is a LIST but its child is not a repeated group", col.element.Name)
			}
			if col.children[0].name != "list" {
				p.errorf("field %s is a LIST but its child is not named \"list\"", col.element.Name)
			}
			if len(col.children[0].children) != 1 {
				p.errorf("field %s.list has %d children", col.element.Name)
			}
			if col.children[0].children[0].element.Name != "element" {
				p.errorf("%s.list has a child but it's called %q, not \"element\"", col.element.Name, col.children[0].children[0].element.Name)
			}
			if rep := col.children[0].children[0].element.GetRepetitionType(); rep != parquet.FieldRepetitionType_OPTIONAL && rep != parquet.FieldRepetitionType_REQUIRED {
				p.errorf("%s.list.element has disallowed repetition type %s", col.element.Name, rep)
			}
		case (col.element.LogicalType != nil && col.element.GetLogicalType().IsSetMAP()) || col.element.GetConvertedType() == parquet.ConvertedType_MAP:
			if col.element.Type != nil {
				p.errorf("field %s is not a group but annotated as MAP", col.element.Name)
			}
			if len(col.children) != 1 {
				p.errorf("field %s is a MAP but has %d children", len(col.children))
			}
			if col.children[0].element.Type != nil || col.children[0].element.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED {
				p.errorf("filed %s is a MAP but its child is not a repeated group", col.element.Name)
			}
			if col.children[0].name != "key_value" {
				p.errorf("field %s is a MAP but its child is not named \"key_value\"", col.element.Name)
			}
			foundKey := false
			foundValue := false
			for _, c := range col.children[0].children {
				switch c.element.Name {
				case "key":
					if c.element.GetRepetitionType() != parquet.FieldRepetitionType_REQUIRED {
						p.errorf("field %s.key_value.key is not of repetition type \"required\"", col.element.Name)
					}
					foundKey = true
				case "value":
					foundValue = true
					// nothing else to check.
				default:
					p.errorf("field %[1]s is a MAP so %[1]s.key_value.%[2]s is not allowed", col.element.Name, c.element.Name)
				}
			}
			if !foundKey {
				p.errorf("field %[1]s is missing %[1]s.key_value.key", col.element.Name)
			}
			if !foundValue {
				p.errorf("field %[1]s is missing %[1]s.key_value.value", col.element.Name)
			}
		case (col.element.LogicalType != nil && col.element.GetLogicalType().IsSetDATE()) || col.element.GetConvertedType() == parquet.ConvertedType_DATE:
			if col.element.GetType() != parquet.Type_INT32 {
				p.errorf("field %[1]s is annotated as DATE but is not an int32", col.element.Name)
			}
		case col.element.LogicalType != nil && col.element.GetLogicalType().IsSetTIMESTAMP():
			if col.element.GetType() != parquet.Type_INT64 {
				p.errorf("field %s is annotated as TIMESTAMP but is not an int64", col.element.Name)
			}
		case col.element.LogicalType != nil && col.element.GetLogicalType().IsSetUUID():
			if col.element.GetType() != parquet.Type_FIXED_LEN_BYTE_ARRAY || col.element.GetTypeLength() != 16 {
				p.errorf("field %s is annotated as UUID but is not a fixed_len_byte_array(16)", col.element.Name)
			}
		case col.element.LogicalType != nil && col.element.GetLogicalType().IsSetENUM():
			if col.element.GetType() != parquet.Type_BYTE_ARRAY {
				p.errorf("field %s is annotated as ENUM but is not a binary", col.element.Name)
			}
		case col.element.LogicalType != nil && col.element.GetLogicalType().IsSetJSON():
			if col.element.GetType() != parquet.Type_BYTE_ARRAY {
				p.errorf("field %s is annotated as JSON but is not a binary", col.element.Name)
			}
		}
	}

	for _, c := range col.children {
		p.validateLogicalTypes(c)
	}
}
