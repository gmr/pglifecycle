"""Generate the schema reference from schemata/*.yml at docs build time.

The schemata are the project format's contract, and they already carry
the prose: nearly every property has a title and a description. This
renders them as one page per schema under `reference/`, plus the
resolved JSON Schema for each at `schemata/<name>.json` so an editor can
validate a project file against a published URL:

    # yaml-language-server: $schema=https://gmr.github.io/pglifecycle/schemata/table.json

Run by the mkdocs gen-files plugin (see mkdocs.yml), which routes
`mkdocs_gen_files.open()` into the build rather than the source tree, so
nothing here is checked in. With `--preview` it writes the same files to ./site-preview instead, for
looking at the output without a full build:

    python3 bin/generate-schema-docs.py --preview
"""

from __future__ import annotations

import collections
import json
import pathlib
import sys

import yaml

SCHEMATA = pathlib.Path(__file__).resolve().parent.parent / 'schemata'

SITE_URL = 'https://gmr.github.io/pglifecycle'

#: JSON Schema draft the bundled files are written against. They
#: declared the draft-agnostic `http://json-schema.org/schema#`, which
#: validators reject, so `src/project/validate.rs` strips it and falls
#: back to its default -- this draft.
DRAFT = 'https://json-schema.org/draft/2020-12/schema'

#: How the reference is organized. Every file in schemata/ must appear
#: in exactly one group, so a new schema fails the build until it is
#: classified rather than going unlisted.
GROUPS: dict[str, list[str]] = {
    'Objects': [
        'project',
        'aggregate',
        'cast',
        'collation',
        'conversion',
        'domain',
        'event_trigger',
        'foreign_data_wrapper',
        'function',
        'index',
        'materialized_view',
        'operator',
        'publication',
        'schema',
        'sequence',
        'server',
        'subscription',
        'table',
        'tablespace',
        'text_search',
        'type',
        'view',
    ],
    'Roles and privileges': ['acls', 'group', 'role', 'user', 'user_mapping'],
    'Shared definitions': [
        'argument',
        'column',
        'constraint',
        'dependencies',
        'foreign_key',
        'trigger',
    ],
    'Per-schema container files': [
        'casts',
        'conversions',
        'operators',
        'types',
    ],
}

#: `reference/index.md` is the section's own landing page, so the Index
#: schema's page needs a name of its own -- writing both to index.md
#: silently lost one of them.
PAGE_NAME = {'index': 'index-schema'}

#: Container files hold an array of objects of another schema; naming
#: the member schema lets the page say so instead of describing the
#: wrapper as though it were the object.
CONTAINER_MEMBER = {
    'casts': 'cast',
    'conversions': 'conversion',
    'operators': 'operator',
    'types': 'type',
}


def page_name(stem: str) -> str:
    """The reference page a schema is written to, without extension."""
    return PAGE_NAME.get(stem, stem)


def load(stem: str) -> dict:
    """The raw schema, with `$package_schema` left in place: for the
    reference it renders as a link, which beats inlining a hundred rows
    of another schema's properties.
    """
    with (SCHEMATA / f'{stem}.yml').open() as handle:
        return yaml.safe_load(handle)


def resolve(node, stack: tuple[str, ...] = ()):
    """Merge `$package_schema` includes, the way
    `src/project/validate.rs` preprocess() does, for the published JSON.
    """
    if isinstance(node, list):
        return [resolve(item, stack) for item in node]
    if not isinstance(node, dict):
        return node
    out: dict = {}
    for key, value in node.items():
        if key in ('$schema', '$id'):
            continue
        if key == '$package_schema':
            if value in stack:  # dependencies -> ... -> dependencies
                continue
            out.update(resolve(load(value), stack + (value,)))
        else:
            out[key] = resolve(value, stack)
    return out


# --- rendering -------------------------------------------------------


def title_of(stem: str, schema: dict) -> str:
    return schema.get('title') or stem.replace('_', ' ').title()


def paragraphs(text: str | None) -> list[str]:
    """Schema descriptions are folded YAML scalars: a blank line is a
    paragraph break, and single newlines are soft wrapping.
    """
    if not text:
        return []
    return [
        ' '.join(block.split())
        for block in text.strip().split('\n\n')
        if block.strip()
    ]


def alternation(pattern: str) -> tuple[list[str], str | None] | None:
    """The literal values a `^(A|B|C)( SUFFIX)?$`-shaped pattern
    accepts, and its optional suffix, so a privilege list reads as
    SELECT / INSERT rather than as a regex. `None` for any pattern that
    is not that shape.
    """
    body = pattern.removeprefix('^').removesuffix('$')
    suffix = None
    if body.endswith(')?') and (cut := body.rfind('(')) > 0:
        suffix = body[cut + 1:-2]
        body = body[:cut]
    if not (body.startswith('(') and body.endswith(')')):
        return None
    values = body[1:-1].split('|')
    metacharacters = set('()[]*+?{}\\.^$|')
    if len(values) < 2 or any(
        metacharacters & set(value) for value in (*values, suffix or '')
    ):
        return None
    return values, suffix


def type_of(schema: dict) -> str:
    """A property's type as a reader wants it: the JSON type, a link to
    the schema it defers to, or the shape of the values it accepts.
    """
    if 'const' in schema:
        return f'`{json.dumps(schema["const"])}`'
    if enum := schema.get('enum'):
        return ' | '.join(f'`{value}`' for value in enum)
    if member := schema.get('$package_schema'):
        return f'[{member}]({page_name(member)}.md)'
    for keyword in ('oneOf', 'anyOf', 'allOf'):
        if branches := schema.get(keyword):
            types = [type_of(branch) for branch in branches]
            seen = list(dict.fromkeys(t for t in types if t))
            if seen:
                return ' | '.join(seen)
    # a bare `pattern` constrains a string; when it is a plain
    # alternation it names the accepted values, which is what a
    # privilege list is
    if 'type' not in schema and (pattern := schema.get('pattern')):
        if parsed := alternation(pattern):
            return ' | '.join(f'`{value}`' for value in parsed[0])
        return '`string`'
    kind = schema.get('type')
    if isinstance(kind, list):
        return ' | '.join(f'`{k}`' for k in kind)
    if kind == 'array':
        item = type_of(schema.get('items') or {})
        if not item:
            return 'array'
        return f'array of ({item})' if '|' in item else f'array of {item}'
    if kind == 'object':
        return 'map' if schema.get('patternProperties') else '`object`'
    return f'`{kind}`' if kind else ''


def notes_of(schema: dict) -> list[str]:
    """Constraints worth stating inline, next to the description."""
    notes = []
    if 'default' in schema:
        notes.append(f'Default: `{json.dumps(schema["default"])}`.')
    if pattern := schema.get('pattern'):
        if parsed := alternation(pattern):
            values, suffix = parsed
            note = 'One of ' + ', '.join(f'`{value}`' for value in values)
            if suffix:
                note += f', each optionally suffixed with `{suffix}`'
            notes.append(f'{note}.')
        else:
            notes.append(f'Must match `{pattern}`.')
    # the key pattern is a machine detail: state it only when no
    # description already says what the keys are (the published JSON
    # carries the pattern either way)
    if not schema.get('description') and (
        names := schema.get('propertyNames')
    ):
        if key_pattern := names.get('pattern'):
            notes.append(f'Keys match `{key_pattern}`.')
    for value in (schema.get('patternProperties') or {}).values():
        if isinstance(value, dict):
            described = type_of(value)
            if described.startswith('array of'):
                described = described.replace('array of', 'an array of', 1)
            notes.append(f'Each value is {described}.')
            notes += notes_of(value)
            break
    for keyword, label in (
        ('minLength', 'Minimum length'),
        ('minItems', 'Minimum items'),
        ('minimum', 'Minimum'),
        ('maximum', 'Maximum'),
    ):
        if keyword in schema:
            notes.append(f'{label}: `{schema[keyword]}`.')
    if schema.get('uniqueItems'):
        notes.append('Entries must be unique.')
    return notes


def cell(schema: dict) -> str:
    """The description column: the property's own prose, its title when
    that adds something the name does not, then its constraints.
    """
    text = paragraphs(schema.get('description'))
    if not text and (title := schema.get('title')):
        text = [title]
    text = [
        para if para.endswith(('.', ':', '!', '?')) else f'{para}.'
        for para in text
    ]
    return ' '.join(text + notes_of(schema)) or '—'


def nested(schema: dict) -> dict:
    """The sub-objects worth their own table: a property that spells out
    an object inline, or an array whose items do.
    """
    out = {}
    for name, prop in (schema.get('properties') or {}).items():
        if not isinstance(prop, dict):
            continue
        for candidate, suffix in ((prop, ''), (prop.get('items'), '[]')):
            if (
                isinstance(candidate, dict)
                and candidate.get('properties')
                and '$package_schema' not in candidate
            ):
                out[f'{name}{suffix}'] = candidate
        for branch in prop.get('anyOf') or prop.get('oneOf') or []:
            if isinstance(branch, dict) and branch.get('properties'):
                out.setdefault(name, branch)
    return out


def property_table(schema: dict) -> list[str]:
    properties = schema.get('properties') or {}
    if not properties:
        return []
    required = set(schema.get('required') or [])
    lines = [
        '| Property | Type | Required | Description |',
        '| --- | --- | --- | --- |',
    ]
    for name, prop in properties.items():
        prop = prop if isinstance(prop, dict) else {}
        # a pipe is a cell separator in a Markdown table, wherever it
        # came from: a type alternation or a description
        columns = [
            f'`{name}`',
            type_of(prop) or '—',
            'yes' if name in required else '—',
            cell(prop),
        ]
        lines.append(
            '| ' + ' | '.join(c.replace('|', '\\|') for c in columns) + ' |'
        )
    return lines


def branch_summary(branch: dict) -> str:
    """One `oneOf` branch as a sentence a reader can act on."""
    parts = []
    if names := branch.get('required'):
        names = [names] if isinstance(names, str) else names
        parts.append(' and '.join(f'`{n}`' for n in names))
    forbidden = (branch.get('not') or {}).get('required') or []
    forbidden = [forbidden] if isinstance(forbidden, str) else forbidden
    if forbidden:
        parts.append(
            'without ' + ' or '.join(f'`{n}`' for n in forbidden)
        )
    if inner := branch.get('anyOf'):
        inner_names = [
            n for b in inner for n in (b.get('properties') or {})
        ]
        if inner_names:
            parts.append(
                'at least one of '
                + ', '.join(f'`{n}`' for n in dict.fromkeys(inner_names))
            )
    return ', '.join(parts)


def exclusivity(schema: dict, level: int = 2) -> list[str]:
    branches = schema.get('oneOf')
    if not branches:
        return []
    summaries = [branch_summary(branch) for branch in branches]
    summaries = [s for s in summaries if s]
    if not summaries:
        return []
    lines = ['', f'{"#" * level} Mutually exclusive forms', '',
             'Exactly one of these must hold:', '']
    lines += [f'- {summary}' for summary in summaries]
    return lines


def page(stem: str) -> str:
    schema = load(stem)
    name = title_of(stem, schema)
    lines = [f'# {name}', '']
    lines += [f'{para}\n' for para in paragraphs(schema.get('description'))]
    if member := CONTAINER_MEMBER.get(stem):
        lines += [
            f'One file per schema, holding every {member} in it. Each'
            f' entry is a [{member}]({page_name(member)}.md).',
            '',
        ]
    if table := property_table(schema):
        lines += ['## Properties', ''] + table
    for label, sub in nested(schema).items():
        lines += ['', f'### `{label}`', '']
        lines += [f'{para}\n' for para in paragraphs(sub.get('description'))]
        lines += property_table(sub)
        lines += exclusivity(sub, level=4)
    lines += exclusivity(schema)
    if schema.get('additionalProperties') is False:
        lines += ['', 'No other properties are accepted.']
    lines += [
        '',
        '---',
        '',
        f'Source: [`schemata/{stem}.yml`]'
        f'(https://github.com/gmr/pglifecycle/blob/main/schemata/{stem}.yml)'
        f' · Resolved JSON Schema: [`{stem}.json`](../schemata/{stem}.json)',
        '',
    ]
    return '\n'.join(lines)


def index_page() -> str:
    lines = [
        '# Schema Reference',
        '',
        'Every object in a pglifecycle project is validated against one of'
        ' these schemas. They are the project format\'s contract; this'
        ' reference is generated from them, so it cannot drift from what'
        ' the tool enforces.',
        '',
        'For where each file lives on disk, see'
        ' [Project Format](../project-format.md).',
        '',
        '## Validating in an editor',
        '',
        'Each schema is published as resolvable JSON Schema. Point'
        ' `yaml-language-server` (VS Code, Neovim, Helix) at the one for'
        ' the file you are editing to get completion and validation as'
        ' you type:',
        '',
        '```yaml',
        f'# yaml-language-server: $schema={SITE_URL}/schemata/table.json',
        '---',
        'name: users',
        'schema: test',
        '```',
        '',
    ]
    for group, stems in GROUPS.items():
        lines += [f'## {group}', '']
        for stem in stems:
            schema = load(stem)
            summary = paragraphs(schema.get('description'))
            first = summary[0] if summary else ''
            lines.append(
                f'- [{title_of(stem, schema)}]({page_name(stem)}.md)'
                + (f' — {first}' if first else '')
            )
        lines.append('')
    return '\n'.join(lines)


def summary_page() -> str:
    """literate-nav SUMMARY for the generated section."""
    lines = ['* [Overview](index.md)']
    for group, stems in GROUPS.items():
        lines.append(f'* {group}')
        for stem in stems:
            lines.append(
                f'    * [{title_of(stem, load(stem))}]'
                f'({page_name(stem)}.md)'
            )
    return '\n'.join(lines) + '\n'


def published_schema(stem: str) -> str:
    schema = resolve(load(stem))
    return json.dumps(
        {
            '$schema': DRAFT,
            '$id': f'{SITE_URL}/schemata/{stem}.json',
            **schema,
        },
        indent=2,
    )


def classified() -> list[str]:
    """Every schema stem, checking the grouping covers them exactly."""
    on_disk = {path.stem for path in SCHEMATA.glob('*.yml')}
    listed = [stem for stems in GROUPS.values() for stem in stems]
    duplicated = [
        stem for stem, count in collections.Counter(listed).items()
        if count > 1
    ]
    unlisted = sorted(on_disk - set(listed))
    unknown = sorted(set(listed) - on_disk)
    problems = []
    if unlisted:
        problems.append(f'not listed in GROUPS: {", ".join(unlisted)}')
    if unknown:
        problems.append(f'listed but missing: {", ".join(unknown)}')
    if duplicated:
        problems.append(f'listed twice: {", ".join(duplicated)}')
    if problems:
        raise SystemExit(
            'bin/generate-schema-docs.py: ' + '; '.join(problems)
        )
    return listed


def generate(writer) -> None:
    stems = classified()
    writer('reference/index.md', index_page())
    writer('reference/SUMMARY.md', summary_page())
    for stem in stems:
        writer(f'reference/{page_name(stem)}.md', page(stem))
        writer(f'schemata/{stem}.json', published_schema(stem))


def main() -> None:
    if '--preview' in sys.argv[1:]:
        root = pathlib.Path('site-preview')

        def writer(path: str, content: str) -> None:
            out = root / path
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content)

        print(f'Writing preview to {root}/', file=sys.stderr)
    else:
        try:
            import mkdocs_gen_files
            from mkdocs_gen_files.editor import FilesEditor
        except ImportError:
            raise SystemExit(
                'bin/generate-schema-docs.py runs under the mkdocs '
                'gen-files plugin; pass --preview to write ./site-preview '
                'instead (see docs/requirements.txt)'
            ) from None

        # outside a build, gen-files falls back to writing into docs/
        # for real, which would check generated pages into the repo
        if pathlib.Path(FilesEditor.current().directory).resolve() == (
            SCHEMATA.parent / 'docs'
        ).resolve():
            raise SystemExit(
                'bin/generate-schema-docs.py: refusing to write into '
                'docs/ -- run it through `mkdocs build`, or pass '
                '--preview to write ./site-preview'
            )

        def writer(path: str, content: str) -> None:
            with mkdocs_gen_files.open(path, 'w') as handle:
                handle.write(content)

    generate(writer)


main()
