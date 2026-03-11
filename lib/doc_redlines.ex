defmodule DocRedlines do
  @moduledoc """
  Fast .doc redline extraction via a Rust NIF.

  This module wraps the native NIF results into Elixir structs for
  a stable public API.
  """

  alias DocRedlines.Native

  defmodule Redline do
    @moduledoc """
    A single redline entry extracted from a .doc file.
    """

    @enforce_keys [:type, :text]
    defstruct type: nil,
              text: nil,
              author: nil,
              timestamp: nil,
              start_cp: nil,
              end_cp: nil,
              paragraph_index: nil,
              char_offset: nil,
              context: nil

    @type t :: %__MODULE__{
            type: :insertion | :deletion,
            text: String.t(),
            author: String.t() | nil,
            timestamp: String.t() | nil,
            start_cp: non_neg_integer(),
            end_cp: non_neg_integer(),
            paragraph_index: non_neg_integer() | nil,
            char_offset: non_neg_integer() | nil,
            context: String.t() | nil
          }
  end

  defmodule Result do
    @moduledoc """
    Redline extraction result.
    """

    @enforce_keys [:redlines]
    defstruct redlines: []

    @type t :: %__MODULE__{redlines: [Redline.t()]}
  end

  @doc """
  Extract redlines from a .doc file path.
  """
  @spec extract_redlines(Path.t()) :: {:ok, Result.t()} | {:error, term()}
  def extract_redlines(doc_path) when is_binary(doc_path) do
    with {:ok, %{redlines: redlines}} <- Native.extract_redlines(doc_path) do
      {:ok, %Result{redlines: Enum.map(redlines, &to_redline/1)}}
    end
  end

  defp to_redline(%{type: type} = redline) do
    %Redline{
      type: type,
      text: Map.get(redline, :text),
      author: Map.get(redline, :author),
      timestamp: Map.get(redline, :timestamp),
      start_cp: Map.get(redline, :start_cp),
      end_cp: Map.get(redline, :end_cp),
      paragraph_index: Map.get(redline, :paragraph_index),
      char_offset: Map.get(redline, :char_offset),
      context: Map.get(redline, :context)
    }
  end
end
