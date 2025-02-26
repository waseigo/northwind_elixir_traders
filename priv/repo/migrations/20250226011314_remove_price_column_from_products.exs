defmodule NorthwindElixirTraders.Repo.Migrations.RemovePriceColumnFromProducts do
  use Ecto.Migration

  def change do
    alter(table(:products), do: remove(:price))
    rename(table(:products), :price_cents, to: :price)
  end
end
