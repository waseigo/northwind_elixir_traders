defmodule NorthwindElixirTraders.Repo.Migrations.AlterCategories do
  use Ecto.Migration

  def change do
    alter table(:categories) do
      modify :id, :identity, primary_key: true, start_value: 999_983, increment: 13, comment: "Deliberately weird auto-incrementing integer primary key values"
      modify :name, :string, null: false, size: 50, comment: "Required; max of original data is 14"
      modify :description, :string, size: 100, comment: "Optional; max of original data is 14"
    end
  end
end
