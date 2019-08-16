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
		case isAlphaNum(r):
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
	validTypes := []string{"binary", "float", "double", "boolean", "int32", "int64", "int96"} // TODO: add more.
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

	// TODO: add support for FIXED_LEN_BYTE_ARRAY
	switch *elem.Type {
	case parquet.Type_BYTE_ARRAY:
		if lt := elem.GetLogicalType(); lt != nil && lt.IsSetSTRING() {
			colStore, err = NewStringStore(parquet.Encoding_PLAIN, true)
		} else {
			colStore, err = NewByteArrayStore(parquet.Encoding_PLAIN, true)
		}
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
	default:
		p.errorf("unsupported type %q", elem.Type.String())
	}
	if err != nil {
		p.errorf("creating column store for type %q failed: %v", elem.Type.String(), err)
	}

	return colStore
}

func (p *schemaParser) parseLogicalType() *parquet.LogicalType {
	p.expect(itemLeftParen)
	p.next()
	p.expect(itemIdentifier)

	typStr := p.token.val

	lt := parquet.NewLogicalType()

	switch strings.ToUpper(typStr) {
	case "STRING":
		lt.STRING = parquet.NewStringType()
	default:
		p.errorf("unsupported logical type %q", typStr)
	}

	p.next()
	// TODO: parse full syntax as found here:
	// https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/schema/MessageTypeParser.java#L169
	p.expect(itemRightParen)

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
