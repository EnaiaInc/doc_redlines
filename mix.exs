defmodule DocRedlines.MixProject do
  use Mix.Project

  @version "0.5.0"
  @source_url "https://github.com/EnaiaInc/doc_redlines"

  def project do
    [
      app: :doc_redlines,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      dialyzer: dialyzer()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:rustler, "~> 0.37.1", optional: true},
      {:rustler_precompiled, "~> 0.8.4"},
      {:pythonx, "~> 0.4.0", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.39.3", only: :dev, runtime: false},
      {:quokka, "~> 2.11", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    "Fast .doc redline extraction via a Rust NIF."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "https://hexdocs.pm/doc_redlines/changelog.html"
      },
      files: [
        "lib",
        "native/doc_redlines_nif/src",
        "native/doc_redlines_nif/Cargo*",
        "native/doc_redlines_nif/Cross.toml",
        "checksum-*.exs",
        "mix.exs",
        "README.md",
        "CHANGELOG.md",
        "LICENSE"
      ]
    ]
  end

  defp docs do
    [
      main: "DocRedlines",
      source_url: @source_url,
      source_ref: "v#{@version}",
      extras: ["CHANGELOG.md"]
    ]
  end

  defp dialyzer do
    [
      plt_ignore_apps: [:mix],
      ignore_warnings: ".dialyzer_ignore.exs"
    ]
  end
end
