defmodule NorthwindElixirTraders.Repo.Migrations.CreateCategories do
  use Ecto.Migration

  def change do
    create table(:categories) do
      add :name, :string
      add :description, :string
    end
  end
end
