defmodule NorthwindElixirTraders.Repo do
  use Ecto.Repo,
    otp_app: :northwind_elixir_traders,
    adapter: Ecto.Adapters.SQLite3
end
