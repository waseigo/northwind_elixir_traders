defmodule NorthwindElixirTraders.Repo.Migrations.ProductBelongsToSupplier do
  use Ecto.Migration

  def change do
    alter table(:products) do
      add :supplier_id, references(:supplier)
    end
  end
end
