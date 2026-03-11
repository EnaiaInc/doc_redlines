defmodule DocRedlines.Native do
  @moduledoc false

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :doc_redlines,
    crate: "doc_redlines_nif",
    base_url: "https://github.com/EnaiaInc/doc_redlines/releases/download/v#{version}",
    force_build: System.get_env("DOC_REDLINES_BUILD") in ["1", "true"],
    version: version,
    nif_versions: ["2.17", "2.16", "2.15"],
    targets: [
      "aarch64-apple-darwin",
      "aarch64-unknown-linux-gnu",
      "x86_64-apple-darwin",
      "x86_64-unknown-linux-gnu"
    ]

  @spec extract_redlines(Path.t()) :: {:ok, map()} | {:error, term()}
  def extract_redlines(doc_path) when is_binary(doc_path) do
    nif_extract_redlines_from_path(doc_path)
  end

  @doc false
  def nif_extract_redlines_from_path(_path), do: :erlang.nif_error(:nif_not_loaded)
end
