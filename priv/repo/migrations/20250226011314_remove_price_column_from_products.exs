defmodule NorthwindElixirTraders.Repo.Migrations.RemovePriceColumnFromProducts do
  use Ecto.Migration

  def change do
    alter(table(:products), do: remove(:price))
  end
end
