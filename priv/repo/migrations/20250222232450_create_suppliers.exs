defmodule NorthwindElixirTraders.Repo.Migrations.CreateSuppliers do
  use Ecto.Migration

  def change do
    create table(:suppliers) do
      add :name, :string, null: false
      add :contact_name, :string
      add :address, :string
      add :city, :string
      add :postal_code, :string
      add :country, :string
      add :phone, :string

      timestamps(type: :utc_datetime)
    end

    create unique_index(:suppliers, [:name])
  end
end
