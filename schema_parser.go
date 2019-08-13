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
	pos  Pos
	val  string
	line int
}

// Pos describes a position
type Pos int

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
	itemNumber
	itemIdentifier
	itemKeyword
	itemMessage
	itemRepeated
	itemOptional
	itemRequired
	itemGroup
)

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
	pos       Pos
	start     Pos
	width     Pos
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
	l.width = Pos(w)
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
	msg   schemaMessage
}

type schemaMessage struct {
	name string
	cols []*schemaColumn
}

type schemaColumn struct {
	name          string
	repType       parquet.FieldRepetitionType
	isGroup       bool
	typ           string
	convertedType string
	fieldID       *int
	children      []*schemaColumn
}

func newSchemaParser(text string) *schemaParser {
	return &schemaParser{
		l: lex(text),
	}
}

func (p *schemaParser) parse() (err error) {
	defer p.recover(&err)

	p.parseMessage()

	p.next()
	p.expect(itemEOF)

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
		p.errorf("expected token type %v, got %v instead", typ, p.token)
	}
	//log.Printf("expect %d successful, token = %v", typ, p.token)
}

func (p *schemaParser) next() {
	p.token = p.l.nextItem()
}

func (p *schemaParser) parseMessage() {
	p.next()
	p.expect(itemMessage)

	p.next()
	p.expect(itemIdentifier)

	p.msg.name = p.token.val

	p.next()
	p.expect(itemLeftBrace)

	p.msg.cols = p.parseMessageBody()

	p.expect(itemRightBrace)
}

func (p *schemaParser) parseMessageBody() []*schemaColumn {
	var cols []*schemaColumn
	for {
		p.next()
		if p.token.typ == itemRightBrace {
			return cols
		}

		cols = append(cols, p.parseColumnDefinition())
	}
}

func (p *schemaParser) parseColumnDefinition() *schemaColumn {
	col := &schemaColumn{}

	switch p.token.typ {
	case itemRepeated:
		col.repType = parquet.FieldRepetitionType_REPEATED
	case itemOptional:
		col.repType = parquet.FieldRepetitionType_OPTIONAL
	case itemRequired:
		col.repType = parquet.FieldRepetitionType_REQUIRED
	default:
		p.errorf("invalid field repetition type %q", p.token.val)
	}

	p.next()
	if p.token.typ == itemGroup {
		col.isGroup = true

		p.next()
		p.expect(itemIdentifier)
		col.name = p.token.val

		p.next()
		if p.token.typ == itemLeftParen {
			col.convertedType = p.parseConvertedType()
		}

		p.next()
		p.expect(itemLeftBrace)

		col.children = p.parseMessageBody()

		p.expect(itemRightBrace)
	} else {
		col.typ = p.token.val
		p.isValidType(col.typ)

		p.next()
		p.expect(itemIdentifier)
		col.name = p.token.val

		p.next()
		if p.token.typ == itemLeftParen {
			col.convertedType = p.parseConvertedType()
			p.next()
		}

		if p.token.typ == itemEqual {
			col.fieldID = p.parseFieldID()
			p.next()
		}

		p.expect(itemSemicolon)
	}

	return col
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

func (p *schemaParser) isValidConvertedType(typ string) {
	validConvertedTypes := []string{"STRING", "LIST", "MAP", "MAP_KEY_VALUE"} // TODO: add more.
	for _, vt := range validConvertedTypes {
		if vt == typ {
			return
		}
	}

	p.errorf("invalid converted type %q", typ)
}

func (p *schemaParser) parseConvertedType() string {
	p.expect(itemLeftParen)
	p.next()
	p.expect(itemIdentifier)

	convertedType := p.token.val
	p.isValidConvertedType(convertedType)

	p.next()
	p.expect(itemRightParen)

	return convertedType
}

func (p *schemaParser) parseFieldID() *int {
	p.expect(itemEqual)
	p.next()
	p.expect(itemNumber)

	i, _ := strconv.Atoi(p.token.val)

	return &i
}
