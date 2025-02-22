defmodule NorthwindElixirTraders.Repo.Migrations.AlterCategories do
  use Ecto.Migration

  def change do
    create table(:newcats, primary_key: false) do
      add :id, :integer, primary_key: true, start_value: 999_983, increment: 13, comment: "Deliberately weird auto-incrementing integer primary key values"
      add :name, :string, null: false, size: 50, comment: "Required; max of original data is 14"
      add :description, :string, size: 100, comment: "Optional; max of original data is 14"
      timestamps(type: :utc_datetime)
    end

    execute("INSERT INTO newcats (id, name, description, inserted_at, updated_at) SELECT id, name, description, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM categories")

    drop table(:categories)

    rename table(:newcats), to: table(:categories)
  end
end
