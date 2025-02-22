import Config

config :northwind_elixir_traders, NorthwindElixirTraders.Repo,
  database: "northwind_elixir_traders_repo",
  username: "user",
  password: "pass",
  hostname: "localhost"

config :northwind_elixir_traders,
  ecto_repos: [NorthwindElixirTraders.Repo]
