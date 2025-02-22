defmodule NorthwindElixirTraders.Repo.Migrations.CategoriesUniqueName do
  use Ecto.Migration

  def change, do: create unique_index(:categories, [:name])
end
