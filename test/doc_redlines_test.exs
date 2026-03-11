defmodule DocRedlinesTest do
  use ExUnit.Case, async: true

  test "extracts from synthetic doc fixture" do
    fixture = Path.expand("fixtures/sample.doc", __DIR__)
    assert File.exists?(fixture)

    assert {:ok, %DocRedlines.Result{redlines: redlines}} =
             DocRedlines.extract_redlines(fixture)

    assert is_list(redlines)
  end
end
