import Config

if Mix.env() == :test do
  config :rustler_precompiled, :force_build, doc_redlines: true
end
