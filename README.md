# DocRedlines

Fast .doc redline extraction via a Rust NIF.

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
- This library does not include or bundle any document samples.
