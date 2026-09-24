# Pagila

`pagila-schema.sql` is the schema of [Pagila](https://github.com/xzilla/pagila),
the PostgreSQL sample database, taken unchanged from commit
`fc7a86771a7ff213597139942f1f57c36125d37d`. Only the schema is included; the
gate needs no rows.

The pagila gate (`just pagila-gate`) runs the round-trip gate on it: load,
`pg_dump`, `pull`, `build`, `pg_restore`, and compare schema-only dumps. It
tests a schema that someone else wrote, where `fixtures/schema.sql` tests what
we thought to test. Pagila's function bodies are not written in the style
`pull` formats them in, so the gate compares routine bodies with whitespace
and semicolons removed (`bin/normalize-routine-bodies`).

To update it, replace `pagila-schema.sql` with the file from a newer commit and
change the commit above.

## License

Portions Copyright (c) 2006-2026 Robert Treat
Portions Copyright (c) 2006 MySQL AB

Pagila is made available under The PostgreSQL License:

> Permission to use, copy, modify, and distribute this software and its
> documentation for any purpose, without fee, and without a written agreement is
> hereby granted, provided that the above copyright notice and this paragraph
> and the following two paragraphs appear in all copies.
>
> IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE TO ANY PARTY FOR
> DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
> PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
> THE AUTHORS OR COPYRIGHT HOLDERS HAVE BEEN ADVISED OF THE POSSIBILITY OF SUCH
> DAMAGE.
>
> THE AUTHORS AND COPYRIGHT HOLDERS SPECIFICALLY DISCLAIM ANY WARRANTIES,
> INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
> FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
> IS" BASIS, AND THE AUTHORS AND COPYRIGHT HOLDERS HAVE NO OBLIGATIONS TO PROVIDE
> MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
