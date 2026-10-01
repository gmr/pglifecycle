//! Split a SQL script, as pg_dumpall writes it, into its statements

/// The statements of a SQL script, each with its semicolon, in their
/// order. A semicolon in a string constant, an escape string constant
/// (`E'...'`), a dollar-quoted string, a quoted identifier or a comment
/// does not end a statement, so a statement can have more than one
/// line. The comments and the psql meta-commands (such as `\restrict`)
/// between the statements are not statements. A meta-command is a line
/// that starts with a backslash before the text of a statement; a line
/// in a string that starts with a backslash is part of the string.
pub fn split_statements(script: &str) -> Vec<&str> {
    let bytes = script.as_bytes();
    let mut statements = Vec::new();
    // the start of the current statement, when it has text
    let mut start: Option<usize> = None;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = line_end(bytes, i);
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = block_comment_end(bytes, i);
                continue;
            }
            b'\\' if start.is_none() => {
                i = line_end(bytes, i);
                continue;
            }
            b';' => {
                if let Some(first) = start.take() {
                    statements.push(&script[first..=i]);
                }
                i += 1;
                continue;
            }
            _ if b.is_ascii_whitespace() => {
                i += 1;
                continue;
            }
            _ => {}
        }
        start.get_or_insert(i);
        i = match b {
            b'\'' => {
                let escape = i > 0
                    && matches!(bytes[i - 1], b'E' | b'e')
                    && (i < 2 || !is_identifier_byte(bytes[i - 2]));
                quoted_end(bytes, i, b'\'', escape)
            }
            b'"' => quoted_end(bytes, i, b'"', false),
            b'$' if i == 0 || !is_identifier_byte(bytes[i - 1]) => {
                dollar_quoted_end(script, i).unwrap_or(i + 1)
            }
            _ => i + 1,
        };
    }
    if let Some(first) = start {
        statements.push(script[first..].trim_end());
    }
    statements
}

/// Whether a byte can be part of an unquoted identifier. Each byte of
/// a character that is not ASCII can be.
fn is_identifier_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

/// The index of the newline that ends the line at `i`, or the end
fn line_end(bytes: &[u8], i: usize) -> usize {
    bytes[i..]
        .iter()
        .position(|&b| b == b'\n')
        .map_or(bytes.len(), |n| i + n)
}

/// The index after the block comment at `i`. Block comments nest.
fn block_comment_end(bytes: &[u8], mut i: usize) -> usize {
    let mut depth = 0;
    while i < bytes.len() {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

/// The index after the string or identifier that `quote` starts at
/// `i`. Two quotes are one quote of the text. In an escape string, a
/// backslash escapes the next byte.
fn quoted_end(bytes: &[u8], mut i: usize, quote: u8, escape: bool) -> usize {
    i += 1;
    while i < bytes.len() {
        if escape && bytes[i] == b'\\' {
            i += 2;
        } else if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

/// The index after the dollar-quoted string at `i` (`$tag$...$tag$`),
/// or `None` if the `$` does not start one
fn dollar_quoted_end(script: &str, i: usize) -> Option<usize> {
    let rest = &script[i + 1..];
    let length = rest.find('$')?;
    let tag = &rest[..length];
    if tag.starts_with(|c: char| c.is_ascii_digit())
        || !tag.bytes().all(|b| is_identifier_byte(b) && b != b'$')
    {
        return None;
    }
    let delimiter = &script[i..i + length + 2];
    let body = i + delimiter.len();
    Some(
        script[body..]
            .find(delimiter)
            .map_or(script.len(), |n| body + n + delimiter.len()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_at_semicolons() {
        assert_eq!(
            split_statements("CREATE ROLE a;\nCREATE ROLE b; DROP ROLE c;"),
            ["CREATE ROLE a;", "CREATE ROLE b;", "DROP ROLE c;"]
        );
    }

    /// pg_dumpall writes a value with a newline on more than one line
    #[test]
    fn keeps_a_string_with_newlines_and_semicolons() {
        let script = "COMMENT ON ROLE a IS 'one;\ntwo';\n\
                      ALTER ROLE a SET application_name TO 'x\n;y';\n";
        assert_eq!(
            split_statements(script),
            [
                "COMMENT ON ROLE a IS 'one;\ntwo';",
                "ALTER ROLE a SET application_name TO 'x\n;y';",
            ]
        );
    }

    #[test]
    fn keeps_quotes_in_a_string() {
        assert_eq!(
            split_statements("SELECT 'it''s; x';SELECT 'a\\';SELECT 2;"),
            ["SELECT 'it''s; x';", "SELECT 'a\\';", "SELECT 2;"]
        );
    }

    /// In an escape string a backslash escapes the next character, and
    /// a line of the string can start with a backslash
    #[test]
    fn keeps_an_escape_string() {
        let script = "COMMENT ON ROLE a IS E'x\\';\n\\\\y; z';\n\
                      COMMENT ON ROLE b IS e'\\\\';\nSELECT 1;";
        assert_eq!(
            split_statements(script),
            [
                "COMMENT ON ROLE a IS E'x\\';\n\\\\y; z';",
                "COMMENT ON ROLE b IS e'\\\\';",
                "SELECT 1;",
            ]
        );
    }

    /// An E at the end of a name does not start an escape string
    #[test]
    fn needs_a_separate_e_for_an_escape_string() {
        assert_eq!(
            split_statements("SELECT name'a\\';SELECT 2;"),
            ["SELECT name'a\\';", "SELECT 2;"]
        );
    }

    #[test]
    fn keeps_a_quoted_identifier() {
        assert_eq!(
            split_statements("CREATE ROLE \"a;\"\"b'\";CREATE ROLE c;"),
            ["CREATE ROLE \"a;\"\"b'\";", "CREATE ROLE c;"]
        );
    }

    #[test]
    fn keeps_a_dollar_quoted_string() {
        let script = "COMMENT ON ROLE a IS $x$it's; $$ ok$x$;\n\
                      COMMENT ON ROLE b IS $$;$$;\n\
                      SELECT $1, a$b;";
        assert_eq!(
            split_statements(script),
            [
                "COMMENT ON ROLE a IS $x$it's; $$ ok$x$;",
                "COMMENT ON ROLE b IS $$;$$;",
                "SELECT $1, a$b;",
            ]
        );
    }

    /// pg_dumpall writes comments, and from PostgreSQL 17 on, the
    /// `\restrict` and `\unrestrict` meta-commands
    #[test]
    fn skips_comments_and_meta_commands() {
        let script = "--\n-- PostgreSQL database cluster dump\n--\n\n\
                      \\restrict abc;def\n\n\
                      SET client_encoding = 'UTF8';\n\
                      /* a; /* nested; */ comment */\n\
                      CREATE ROLE a; -- the role; a comment\n\
                      CREATE ROLE b -- a comment; in a statement\n;\n\
                      \\unrestrict abc;def\n";
        assert_eq!(
            split_statements(script),
            [
                "SET client_encoding = 'UTF8';",
                "CREATE ROLE a;",
                "CREATE ROLE b -- a comment; in a statement\n;",
            ]
        );
    }

    #[test]
    fn keeps_a_last_statement_with_no_semicolon() {
        assert_eq!(
            split_statements("CREATE ROLE a;\nCREATE ROLE b\n"),
            ["CREATE ROLE a;", "CREATE ROLE b"]
        );
        assert!(split_statements("  \n-- none\n").is_empty());
    }
}
