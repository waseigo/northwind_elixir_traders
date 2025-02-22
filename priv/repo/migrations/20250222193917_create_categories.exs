defmodule NorthwindElixirTraders.Repo.Migrations.CreateCategories do
  use Ecto.Migration

  def change do
    create table(:categories, primary_key: false) do
      add :id, :identity, primary_key: true, start_value: 999_983, increment: 13, comment: "deliberately weird auto-incrementing integer primary key values"
      add :name, :string, null: false, size: 50, comment: "Required; max size of original data is 14"
      add :description, :string, null: true, size: 100, comment: "Optional; max size of original data is 58"

      timestamps(type: :utc_datetime)
    end

    create unique_index(:categories, [:id])
  end
end
