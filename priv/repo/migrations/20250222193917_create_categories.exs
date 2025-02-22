defmodule NorthwindElixirTraders.Repo.Migrations.CreateCategories do
  use Ecto.Migration

  def change do
    create table(:categories, primary_key: false) do
      add :name, :string
      add :description, :string

      timestamps(type: :utc_datetime)
    end
  end
end
