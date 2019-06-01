package tinydb.parse;

import java.util.*;

import tinydb.util.BadSyntaxException;

import java.io.*;

public class Lexer {
	private Collection<String> keywords;
	private StreamTokenizer tok;

	public Lexer(String s) {
		initKeywords();
		tok = new StreamTokenizer(new StringReader(s));
		tok.ordinaryChar('.');
		tok.lowerCaseMode(true); // ids and keywords are converted
		nextToken();
	}
	
	private void initKeywords() {
		keywords = Arrays.asList("select", "from", "where", "and", "or", "join", "insert", "into", "values", "delete",
				"update", "set", "create", "table", "view", "as", "index", "on", "drop", "use", "database", "show",
				"databases", "int", "varchar", "long", "float", "double", "string", "not", "null", "primary", "key",
				"natural");
	}

//Methods to check the status of the current token

	public boolean matchDelim(char d) {
		return d == (char) tok.ttype;
	}

	public boolean matchIntConstant() {
		return tok.ttype == StreamTokenizer.TT_NUMBER && tok.nval == (int) tok.nval;
	}

	public boolean matchIntLongConstant() {
		return tok.ttype == StreamTokenizer.TT_NUMBER && tok.nval == (long) tok.nval;
	}

	public boolean matchFloatDoubleConstant() {
		return tok.ttype == StreamTokenizer.TT_NUMBER && tok.nval == (double) tok.nval;
	}

	public boolean matchStringConstant() {
		return '\'' == (char) tok.ttype;
	}
	
	public boolean matchKeyword(String w) {
		return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
	}

	public boolean matchId() {
		return tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
	}

	//Methods to "eat" the current token

	public void eatDelim(char d) {
		if (!matchDelim(d))
			throw new BadSyntaxException();
		nextToken();
	}

	public int eatIntConstant() {
		if (!matchIntConstant())
			throw new BadSyntaxException();
		int i = (int) tok.nval;
		nextToken();
		return i;
	}

	public long eatIntLongConstant() {
		if (!matchIntLongConstant())
			throw new BadSyntaxException();
		long i = (long) tok.nval;
		nextToken();
		return i;
	}

	public double eatFloatDoubleConstant() {
		if (!matchFloatDoubleConstant())
			throw new BadSyntaxException();
		double i = (double) tok.nval;
		nextToken();
		return i;
	}

	public String eatStringConstant() {
		if (!matchStringConstant())
			throw new BadSyntaxException();
		String s = tok.sval; // constants are not converted to lower case
		nextToken();
		return s;
	}

	public void eatKeyword(String w) {
		if (!matchKeyword(w))
			throw new BadSyntaxException();
		nextToken();
	}

	public String eatId() {
		if (!matchId())
			throw new BadSyntaxException();
		String s = tok.sval;
		nextToken();
		return s;
	}

	private void nextToken() {
		try {
			tok.nextToken();
		} catch (IOException e) {
			throw new BadSyntaxException();
		}
	}
}