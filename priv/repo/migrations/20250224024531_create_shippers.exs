defmodule NorthwindElixirTraders.Repo.Migrations.CreateShippers do
  use Ecto.Migration

  def change do
    create table(:shippers) do
      add :name, :string, null: false
      add :phone, :string

      timestamps(type: :utc_datetime)
    end

    create unique_index(:shippers, [:name])
  end
end
