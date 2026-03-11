[
  # Mix.Task callbacks aren't visible to Dialyzer at compile time
  ~r/lib\/mix\/tasks\/pdf_redlines.bench.ex/,
  {:unknown_function, ~r/Mix\.Task\.run\/1/},
  {:unknown_function, ~r/Mix\.raise\/1/}
]
