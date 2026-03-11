[![Hex.pm](https://img.shields.io/hexpm/v/doc_redlines)](https://hex.pm/packages/doc_redlines)
[![Hexdocs.pm](https://img.shields.io/badge/docs-hexdocs.pm-purple)](https://hexdocs.pm/doc_redlines)
[![Github.com](https://github.com/EnaiaInc/doc_redlines/actions/workflows/ci.yml/badge.svg)](https://github.com/EnaiaInc/doc_redlines/actions)

# DocRedlines

Fast legacy `.doc` redline extraction via a Rust NIF. Built to closely match
LibreOffice’s track-changes output and achieve near-parity on real-world
documents.

## Install

Add to `mix.exs`:

```elixir
def deps do
  [
    {:doc_redlines, "~> 0.5"}
  ]
end
```

## Usage

```elixir
{:ok, result} = DocRedlines.extract_redlines("/absolute/path/to/file.doc")
redlines = result.redlines
```

Each redline entry includes:
- `type` (`:insertion` or `:deletion`)
- `text`
- `author`
- `timestamp`
- `start_cp`, `end_cp`
- optional `paragraph_index`, `char_offset`, `context`

## Notes

- Input files must be legacy Word `.doc` format.
- Precompiled NIFs are published in GitHub Releases; set `DOC_REDLINES_BUILD=1`
  to force a local build.
- This library does not include or bundle any document samples.

## Development

To force a local build of the NIF:

```bash
DOC_REDLINES_BUILD=1 mix test
```

To compile all Rust targets locally:

```bash
RUSTLER_PRECOMPILED_FORCE_BUILD_ALL=1 mix compile
```

## License

MIT
